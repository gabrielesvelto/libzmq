/*
    Copyright (c) 2007-2017 Contributors as noted in the AUTHORS file

    This file is part of libzmq, the ZeroMQ core engine in C++.

    libzmq is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License (LGPL) as published
    by the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    As a special exception, the Contributors give you permission to link
    this library with independent modules to produce an executable,
    regardless of the license terms of these independent modules, and to
    copy and distribute the resulting executable under terms of your choice,
    provided that you also meet, for each linked independent module, the
    terms and conditions of the license of that module. An independent
    module is a module which is not derived from or based on this library.
    If you modify this library, you must extend this exception to your
    version of the library.

    libzmq is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
    License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "platform.hpp"

#if defined ZMQ_HAVE_RDMA

#if !defined ZMQ_HAVE_WINDOWS
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#endif

#include <algorithm>
#include <string>
#include <new>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include "rdma_engine.hpp"
#include "io_thread.hpp"
#include "session_base.hpp"
#include "config.hpp"
#include "err.hpp"

zmq::rdma_engine_t::rdma_engine_t (rdma_cm_id *id_, const options_t &options_,
    bool active_) :
    id (id_),
    pd (NULL),
    comp_channel (NULL),
    cq (NULL),
    qp (NULL),
    rcv_queue_depth (0),
    snd_queue_depth (0),
    rcv_buffer_size (est_buffer_size (def_rcv_queue_depth, options_.rcvbuf)),
    rcv_buffer (NULL),
    rcv_mr (NULL),
    snd_buffer_size (est_buffer_size (def_snd_queue_depth, options_.sndbuf)),
    snd_buffer (NULL),
    snd_mr (NULL),
    snd_avail_off (0),
    snd_pending_off (0),
    snd_posted_off (0),
    snd_wr_id (0),
    snd_compl_wr_id (0),
    active_p (active_),
    decoder (in_batch_size, options_.maxmsgsize),
    encoder (out_batch_size),
    session (NULL),
    leftover_session (NULL),
    options (options_),
    plugged (false)
{

}

//  Carefully destroy every component of the RDMA engine, the object might be
//  only partially initialized so we have to be careful not to accidentally try
//  destroy a non-initialized component which would lead to a crash.

zmq::rdma_engine_t::~rdma_engine_t ()
{
    int rc;

    if (qp) {
        rdma_destroy_qp (id);
        qp = NULL;
    }

    if (cq) {
        rc = ibv_destroy_cq (cq);
        posix_assert (rc);
    }

    if (comp_channel) {
        rc = ibv_destroy_comp_channel (comp_channel);
        posix_assert (rc);
    }

    if (snd_mr) {
        rc = ibv_dereg_mr (snd_mr);
        posix_assert (rc);
    }

    if (rcv_mr) {
        rc = ibv_dereg_mr (rcv_mr);
        posix_assert (rc);
    }

    if (pd) {
        rc = ibv_dealloc_pd (pd);
        posix_assert (rc);
    }

    delete [] snd_buffer;
    delete [] rcv_buffer;

    if (active_p) {
        rdma_event_channel *channel = id->channel;

        rc = rdma_destroy_id (id);
        errno_assert (rc == 0);
        rdma_destroy_event_channel (channel);
    }
}

int zmq::rdma_engine_t::init ()
{
    ibv_qp_init_attr qp_init_attr;
    int rc;

    //  Allocate the protection domain.
    pd = ibv_alloc_pd (id->verbs);
    errno_assert (pd);

    //  Create the receive buffer and register it.
    rcv_buffer = new (std::nothrow) unsigned char[rcv_buffer_size];
    alloc_assert (rcv_buffer);
    rcv_mr = ibv_reg_mr (pd, rcv_buffer, rcv_buffer_size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    if (rcv_mr == NULL)
        return errno;

    //  Create the send buffer and register it.
    snd_buffer = new (std::nothrow) unsigned char[snd_buffer_size];
    alloc_assert (snd_buffer);
    snd_mr = ibv_reg_mr (pd, snd_buffer, snd_buffer_size,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    if (snd_mr == NULL)
        return errno;

    //  Create the completion channel.
    comp_channel = ibv_create_comp_channel (id->verbs);
    errno_assert (comp_channel);

    //  Create the completion queue.
    rcv_queue_depth = est_queue_depth (def_rcv_queue_depth, rcv_buffer_size);
    snd_queue_depth = est_queue_depth (def_snd_queue_depth, snd_buffer_size);
    cq = ibv_create_cq (id->verbs, rcv_queue_depth + snd_queue_depth, NULL,
        comp_channel, 0);

    if (cq == NULL)
        return errno;

    //  Make all the completion events appear on the completion channel.
    rc = ibv_req_notify_cq (cq, 0);
    posix_assert (rc);

    //  Create the queue-pair, we'll use a shared completion queue for send and
    //  receive operations and the watermarks to adjust the depth of the
    //  transfer and receive queues.
    memset (&qp_init_attr, 0, sizeof (ibv_qp_init_attr));

    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;

    qp_init_attr.cap.max_send_wr = snd_queue_depth;
    qp_init_attr.cap.max_recv_wr = rcv_queue_depth;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_inline_data = 400;
    qp_init_attr.sq_sig_all = 1;

    rc = rdma_create_qp (id, pd, &qp_init_attr);
    qp = id->qp;

    if (rc != 0)
        return rc;

    //  TODO: Post the receive operations before connecting to the other
    //  end-point to prevent the messages from being refused.
    return 0;
}

void zmq::rdma_engine_t::plug (io_thread_t *io_thread_,
    session_base_t *session_)
{
    zmq_assert (!plugged);
    plugged = true;
    leftover_session = NULL;

    //  Connect to session object.
    zmq_assert (!session);
    zmq_assert (session_);
    encoder.set_session (session_);
    decoder.set_session (session_);
    session = session_;

    //  Connect to I/O threads poller object, we use the completion channel's
    //  file descriptor for signaling events.
    io_object_t::plug (io_thread_);
    qp_handle = add_fd (comp_channel->fd);
    set_pollin (qp_handle);

    //  If we're on the active side of a connection we also add the RDMA
    //  connection manager ID to the poller set.
    if (active_p) {
        id_handle = add_fd (id->channel->fd);
        set_pollin (id_handle);
    }
}

void zmq::rdma_engine_t::unplug ()
{
    zmq_assert (plugged);
    plugged = false;

    //  Cancel all fd subscriptions.
    rm_fd (qp_handle);

    if (active_p) {
        rm_fd (id_handle);
    }

    //  Disconnect from I/O threads poller object.
    io_object_t::unplug ();

    //  Disconnect from session object.
    encoder.set_session (NULL);
    decoder.set_session (NULL);
    leftover_session = session;
    session = NULL;
}

void zmq::rdma_engine_t::terminate ()
{
    int rc = rdma_disconnect (id);
    errno_assert (rc == 0);
}

//  TODO: This event will be completely overhauled as it will need to handle
//  both reads and writes.

void zmq::rdma_engine_t::in_event (fd_t fd_)
{
    bool disconnection = false;

    //  If there's no data to process in the buffer...
    if (likely(fd_ == comp_channel->fd)) {
        //  Queue-pair completion-queue event received.
        qp_event ();
    }
    else {
        //  RDMA ID event received.
        zmq_assert (fd_ == id->channel->fd);
        disconnection = id_event ();
    }


    if (disconnection) {
        //  We received a disconnect event, we can tear down the object cleanly.
        unplug ();
        delete this;
    }
}

void zmq::rdma_engine_t::activate_out ()
{
    //  We set POLLIN as completed write events are read from the completion
    //  queue and there is no POLLOUT event to indicate when we can send data.
    set_pollin (qp_handle);

    //  Speculative write: The assumption is that at the moment new message
    //  was sent by the user the queue-pair is probably available for writing.

    fill_snd_buffer ();
    post_send_wrs ();
}

void zmq::rdma_engine_t::activate_in ()
{
    set_pollin (qp_handle);

    //  Speculative read.

    //  TODO: Implement the speculative read.
}

void zmq::rdma_engine_t::qp_event ()
{
    //  TODO: Poll the completion queue.

    //  Send data.
    fill_snd_buffer ();
    post_send_wrs ();
}

void zmq::rdma_engine_t::id_event ()
{
    rdma_cm_event *event = NULL;
    bool disconnection = false;
    int rc;

    rc = rdma_get_cm_event (id->channel, &event);
    errno_assert (rc == 0);

    switch (event->event) {

    case RDMA_CM_EVENT_DISCONNECTED:
        disconnection = true;
        break;

    default:
        ; //  Ignore all other events.

    }

    rc = rdma_ack_cm_event (event);
    errno_assert (rc == 0);

    return disconnection;
}

void zmq::rdma_engine_t::fill_snd_buffer ()
{
    size_t size = snd_avail ();
    unsigned char *buf = snd_buffer + snd_avail_off;

    encoder.get_data (&buf, &size); //  Zero-copy operation.
    update_snd_avail (size);
}

void zmq::rdma_engine_t::post_send_wrs ()
{
    size_t pending = std::min (snd_pending (),
                               snd_wrs_avail () * def_datagram_size);
    int rc;

    //  TODO: Create multiple work requests at once and issue all of them with
    //  with a single ibv_post_send () command.

    while (pending > 0) {
        size_t wr_size = std::min (pending, (size_t)def_datagram_size);
        ibv_send_wr *bad_wr = NULL;
        ibv_send_wr wr;
        ibv_sge sge;

        //  Initalize the work request.

        //  We store both the WR number and the buffer offset in the ID.
        wr.wr_id = ((uint64_t)snd_wr_id << 32) | (snd_pending_off + wr_size);
        wr.next = NULL;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_SEND;
        wr.send_flags = 0; //  TODO: Use inline data for small transfers.

        sge.addr = (uint64_t)(snd_buffer + snd_pending_off);
        sge.length = wr_size;
        sge.lkey = snd_mr->lkey;

        //  Post the send operation.
        rc = ibv_post_send (qp, &wr, &bad_wr);
        posix_assert (rc);

        //  Adjust the pending buffer pointer and WR number.
        update_snd_pending (wr_size);
        snd_wr_id++;
    }
}

void zmq::rdma_engine_t::error ()
{
    zmq_assert (session);
    session->detach ();
    unplug ();
    delete this;
}

uint32_t zmq::rdma_engine_t::snd_avail ()
{
    if (snd_avail_off < snd_posted_off) {
        return snd_posted_off - snd_avail_off;
    } else {
        return snd_buffer_size - snd_avail_off;
    }
}

void zmq::rdma_engine_t::update_snd_avail (uint32_t size_)
{
    if (snd_avail_off + size_ == snd_buffer_size) {
        snd_avail_off = 0; //  Wrap around.
    } else {
        snd_avail_off += size_;
    }
}

uint32_t zmq::rdma_engine_t::snd_pending ()
{
    if (snd_pending_off < snd_avail_off) {
        return snd_avail_off - snd_pending_off;
    } else {
        return snd_buffer_size - snd_pending_off;
    }
}

void zmq::rdma_engine_t::update_snd_pending (uint32_t size_)
{
    if (snd_pending_off + size_ == snd_buffer_size) {
        snd_pending_off = 0; //  Wrap around.
    } else {
        snd_pending_off += size_;
    }
}

void zmq::rdma_engine_t::update_snd_posted (uint32_t off_)
{
    if (off_ == snd_buffer_size) {
        off_ = 0; //  Wrap around.
    } else {
        snd_posted_off = off_;
    }
}

//  Return the number of available send work-requests.

uint32_t zmq::rdma_engine_t::snd_wrs_avail ()
{
    return snd_wr_id - snd_compl_wr_id;
}

//  Returns a reasonable size for the send/receive buffers depending on the
//  corresponding option. queue_depth is the default queue depth for the buffer
//  and buf_size desired size of the associated buffer.

int zmq::rdma_engine_t::est_buffer_size (int def_qd_, int buf_size_)
{
    int size = def_qd_ * def_datagram_size;

    if (buf_size_ > 0) {
        //  Round the buffer to the datagram size.
        size = ((buf_size_ + def_datagram_size - 1) / def_datagram_size)
            * def_datagram_size;
    }

    return size;
}

//  Returns a reasonable depth for the send/receive queue depending on the
//  send/receive buffer size. The number of pending operations will also be
//  checked against the amount supported by the HCA. queue_depth is the desired
//  queue depth, buf_size the size of the associated buffer.

int zmq::rdma_engine_t::est_queue_depth (int queue_depth_, int buf_size_)
{
    ibv_device_attr attr;
    int rc;
    int depth = std::max (queue_depth_, buf_size_ / def_datagram_size);

    //  Retrieve the maximum number of WRs supported by the HCA.
    rc = ibv_query_device (id->verbs, &attr);
    posix_assert (rc);

    return std::min (depth, attr.max_qp_wr / 2);
}

#endif //  ZMQ_HAVE_RDMA
