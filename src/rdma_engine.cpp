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
    rcv_sizes (NULL),
    rcv_off (0),
    rcv_pending_id (0),
    rcv_posted_id (0),
    rcv_avail_id (0),
    snd_avail_off (0),
    snd_pending_off (0),
    snd_posted_off (0),
    avail_snd_wr (0),
    posted_snd_wr (1), //  Skip the first id so that it's different from compl.
    compl_snd_wr (0),
    snd_enabled (true),
    rcv_enabled (true),
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

    rcv_sizes =
        new (std::nothrow) uint32_t[rcv_buffer_size / def_datagram_size];

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

    //  Create the completion queue, it's depth is calculated based on the
    //  size of the send and receive buffers.
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
    //  receive operations.
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

    //  Post immediately all the available receive work requests, this will
    //  prevent incoming packets from being dropped after the connection is
    //  established. This could happen if the sender would send a message
    //  before the receiver could post its receive work requests.
    post_rcv_wrs ();

    //  The number of available send work requests matches the queue depth.
    avail_snd_wr = snd_queue_depth;

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
    snd_enabled = true;
    rcv_enabled = true;

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
    snd_enabled = true;

    //  Speculative write, the assumption is that at the moment new message
    //  was sent by the user the queue-pair is probably available for writing.
    fill_snd_buffer ();
    post_snd_wrs ();
}

void zmq::rdma_engine_t::activate_in ()
{
    set_pollin (qp_handle);
    rcv_enabled = true;

    //  Immediately read from the receive buffer if there's some pending data.
    drain_rcv_buffer ();
    post_rcv_wrs ();
}

void zmq::rdma_engine_t::qp_event ()
{
    int rc;
    ibv_cq *cq;
    void *cq_context;
    ibv_wc wc;

    //  Retrieve the completion queue event.
    rc = ibv_get_cq_event (comp_channel, &cq, &cq_context);
    posix_assert (rc);

    //  Acknowledge the events.
    //  TODO: This is expensive and should be batched up for multiple events.
    ibv_ack_cq_events (cq, 1);

    //  Re-arm the notification mechanism.
    rc = ibv_req_notify_cq (cq, 0);
    posix_assert (rc);

    //  Finally poll the completion queue.
    //  TODO: Retrieve multiple WCs at the same time, not just one.
    while ((rc = ibv_poll_cq (cq, 1, &wc)) > 0) {
        zmq_assert (rc != -1);

        if (unlikely (wc.status != IBV_WC_SUCCESS)) {
            if (wc.status == IBV_WC_WR_FLUSH_ERR) {
                //  We're disconnecting, everything's fine.
                return;
            } else {
                //  TODO: Some errors are not critical and should be handled.
                zmq_assert (false);
            }
        }

        if (wc.opcode & IBV_WC_RECV) {
            //  Record the completed receive operation by adding its size
            //  to the list of pending received chunks.
            rcv_sizes[wc.wr_id] = wc.byte_len;
            update_rcv_posted_id (1);
        } else {
            //  Extract the send work request ID and the buffer offset from the
            //  work completion event wr_id field.
            uint32_t wr_id = wc.wr_id >> 32;
            uint32_t off = wc.wr_id & 0xffffffff;

            //  Record the completed send operations.
            avail_snd_wr += (wr_id - compl_snd_wr);
            compl_snd_wr = wr_id;
            update_snd_posted (off);
        }
    }

    //  Send data.
    fill_snd_buffer ();
    post_snd_wrs ();

    //  Post available read operations.
    post_rcv_wrs ();
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

    if (size == 0) {
        //  Signal that there isn't any more data to send.
        snd_enabled = false;

        if (!snd_enabled && !rcv_enabled) {
            //  Stop polling on the completion queue.
            reset_pollin (qp_handle);
        }
    }
    else
        update_snd_avail (size);
}

void zmq::rdma_engine_t::post_snd_wrs ()
{
    size_t pending = std::min (snd_pending (),
        avail_snd_wr * def_datagram_size);
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
        wr.wr_id = ((uint64_t)posted_snd_wr << 32)
            | (snd_pending_off + wr_size);
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
        posted_snd_wr++;
        avail_snd_wr--;
    }
}

void zmq::rdma_engine_t::post_rcv_wrs ()
{
    int rc;

    //  TODO: Create multiple work requests at once and issue all of them with
    //  with a single ibv_post_recv () command.

    while (rcv_wrs_avail () > 0) {
        ibv_recv_wr *bad_wr;
        ibv_recv_wr wr;
        ibv_sge sge;

        wr.wr_id = rcv_avail_id;
        wr.next = NULL;
        wr.sg_list = &sge;
        wr.num_sge = 1;

        sge.addr = (uint64_t)rcv_buffer + (rcv_avail_id * def_datagram_size);
        sge.length = def_datagram_size;
        sge.lkey = rcv_mr->lkey;

        //  Post the receive operation.
        rc = ibv_post_recv (qp, &wr, &bad_wr);
        posix_assert (rc);

        //  Adjust the pending buffer pointer and WR number.
        update_rcv_avail_id (1);
    }
}

void zmq::rdma_engine_t::drain_rcv_buffer ()
{
    while (rcv_pending () > 0) {
        unsigned char *buff = rcv_buffer + (rcv_pending_id * def_datagram_size);
        size_t size = rcv_sizes[rcv_pending_id];
        size_t processed = decoder.process_buffer (buff + rcv_off, size);

        if (processed < size) {
            //  Record some state so we can resume decoding later.
            rcv_off += processed;
            rcv_sizes[rcv_pending_id] = size - processed;
            rcv_enabled = false;

            if (!snd_enabled && !rcv_enabled) {
                //  Stop polling on the completion queue.
                reset_pollin (qp_handle);
            }

            break;
        } else {
            //  Reset the in-datagram offset and jump to the next datagram.
            rcv_off = 0;
            rcv_pending_id++;

            //  Wrap around if we went past the end of the buffer.
            if (rcv_pending_id > (rcv_buffer_size / def_datagram_size)) {
                rcv_pending_id -= (rcv_buffer_size / def_datagram_size);
            }
        }
    }
}

void zmq::rdma_engine_t::error ()
{
    zmq_assert (session);
    session->detach ();
    unplug ();
    delete this;
}

//  Returns the number of received datagrams waiting to be decoded.

uint32_t zmq::rdma_engine_t::rcv_pending ()
{
    if (rcv_pending_id < rcv_posted_id) {
        return rcv_posted_id - rcv_pending_id;
    } else {
        return ((rcv_buffer_size / def_datagram_size) - rcv_pending_id)
            + rcv_posted_id;
    }
}


//  Returns the number of available receive work-requests.

uint32_t zmq::rdma_engine_t::rcv_wrs_avail ()
{
    if (rcv_avail_id < rcv_pending_id) {
        return rcv_pending_id - rcv_avail_id;
    } else {
        return ((rcv_buffer_size / def_datagram_size) - rcv_avail_id)
            + rcv_pending_id;
    }
}

//  Update the available receive work request id to reflect the number of work
//  requests that were posted.

void zmq::rdma_engine_t::update_rcv_avail_id (uint32_t posted_)
{
    rcv_avail_id += posted_;

    //  Wrap around if we went past the end of the buffer.
    if (rcv_avail_id > (rcv_buffer_size / def_datagram_size)) {
        rcv_avail_id -= (rcv_buffer_size / def_datagram_size);
    }
}

//  Update the posted receive work request id to reflect the number of work
//  requests that were received.

void zmq::rdma_engine_t::update_rcv_posted_id (uint32_t received_)
{
    rcv_posted_id += received_;

    //  Wrap around if we went past the end of the buffer.
    if (rcv_posted_id > (rcv_buffer_size / def_datagram_size)) {
        rcv_posted_id -= (rcv_buffer_size / def_datagram_size);
    }
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
//  queue depth, buf_size the size of the associated buffer. The returned
//  buffer is always a multiple of def_datagram_size.

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
