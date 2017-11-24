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
    tx_queue_depth (0),
    rx_queue_depth (0),
    rx_buffer_size (0),
    rx_buffer (NULL),
    rx_mr (NULL),
    active_p (active_),
    initialized_p (false),
    inpos (NULL),
    insize (0),
    decoder (in_batch_size, options_.maxmsgsize),
    outpos (NULL),
    outsize (0),
    encoder (out_batch_size),
    session (NULL),
    leftover_session (NULL),
    options (options_),
    plugged (false)
{
    ibv_qp_init_attr qp_init_attr;
    int rc;

    //  Allocate the protection domain.
    pd = ibv_alloc_pd (id_->verbs);
    errno_assert (pd);

    //  Create the receive buffer and register it.
    rx_buffer_size = est_rx_buffer_size ();
    rx_buffer = new (std::nothrow) char[options.rcvbuf];
    alloc_assert (rx_buffer);
    rx_mr = ibv_reg_mr (pd, rx_buffer, options.rcvbuf,
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

    if (rx_mr == NULL)
        return;

    //  Create the completion channel.
    comp_channel = ibv_create_comp_channel (id_->verbs);
    errno_assert (comp_channel);

    //  Create the completion queue.
    tx_queue_depth = est_tx_queue_depth ();
    rx_queue_depth = est_rx_queue_depth ();
    cq = ibv_create_cq (id_->verbs, tx_queue_depth + rx_queue_depth, NULL,
        comp_channel, 0);

    if (cq == NULL)
        return;

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

    qp_init_attr.cap.max_send_wr = tx_queue_depth;
    qp_init_attr.cap.max_recv_wr = rx_queue_depth;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    qp_init_attr.cap.max_inline_data = 400;
    qp_init_attr.sq_sig_all = 1;

    rc = rdma_create_qp (id, pd, &qp_init_attr);
    qp = id->qp;

    if (rc)
        return;

    //  TODO: Post the receive operations before connecting to the other
    //  end-point to prevent the messages from being refused.

    //  The object was successfully initialized, flag it as valid
    initialized_p = true;
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

    if (rx_mr) {
        rc = ibv_dereg_mr (rx_mr);
        posix_assert (rc);
    }

    delete [] rx_buffer;

    if (active_p) {
        rdma_event_channel *channel = id->channel;

        rc = rdma_destroy_id (id);
        errno_assert (rc == 0);
        rdma_destroy_event_channel (channel);
    }
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
    cq_handle = add_fd (comp_channel->fd);
    set_pollin (cq_handle);

    //  If we're on the active side of a connection we also add the RDMA
    //  connection manager ID to the poller set.
    if (active_p) {
        cc_handle = add_fd (id->channel->fd);
        set_pollin (cc_handle);
    }

    //  Flush all the data that may have been already received downstream.
    in_event ();
}

void zmq::rdma_engine_t::unplug ()
{
    zmq_assert (plugged);
    plugged = false;

    //  Cancel all fd subscriptions.
    rm_fd (cq_handle);

    if (active_p) {
        rm_fd (cc_handle);
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
    unplug ();
    delete this;
}

//  TODO: This event will be completely overhauled as it will need to handle
//  both reads and writes.

void zmq::rdma_engine_t::in_event ()
{
    bool disconnection = false;

    //  If there's no data to process in the buffer...
    if (!insize) {

        //  Retrieve the buffer and read as much data as possible.
        //  Note that buffer can be arbitrarily large. However, we assume
        //  the underlying TCP layer has fixed buffer size and thus the
        //  number of bytes read will be always limited.
        decoder.get_buffer (&inpos, &insize);
        insize = read (inpos, insize);

        //  Check whether the peer has closed the connection.
        if (insize == (size_t) -1) {
            insize = 0;
            disconnection = true;
        }
    }

    //  Push the data to the decoder.
    size_t processed = decoder.process_buffer (inpos, insize);

    if (unlikely (processed == (size_t) -1)) {
        disconnection = true;
    }
    else {

        //  Stop polling for input if we got stuck.
        if (processed < insize) {

            //  This may happen if queue limits are in effect.
            if (plugged)
                reset_pollin (cq_handle);
        }

        //  Adjust the buffer.
        inpos += processed;
        insize -= processed;
    }

    //  Flush all messages the decoder may have produced.
    //  If IO handler has unplugged engine, flush transient IO handler.
    if (unlikely (!plugged)) {
        zmq_assert (leftover_session);
        leftover_session->flush ();
    } else {
        session->flush ();
    }

    if (session && disconnection)
        error ();
}

//  TODO: We will get rid of this method as it will never be called, its
//  functionality will be merged into in_event().

void zmq::rdma_engine_t::out_event ()
{
    //  If write buffer is empty, try to read new data from the encoder.
    if (!outsize) {

        outpos = NULL;
        encoder.get_data (&outpos, &outsize);

        //  If IO handler has unplugged engine, flush transient IO handler.
        if (unlikely (!plugged)) {
            zmq_assert (leftover_session);
            leftover_session->flush ();
            return;
        }

        //  If there is no data to send return
        if (outsize == 0) {
            return;
        }
    }

    //  If there are any data to write in write buffer, write as much as
    //  possible to the socket. Note that amount of data to write can be
    //  arbitratily large. However, we assume that underlying TCP layer has
    //  limited transmission buffer and thus the actual number of bytes
    //  written should be reasonably modest.
    int nbytes = write (outpos, outsize);

    //  Handle problems with the connection.
    if (nbytes == -1) {
        error ();
        return;
    }

    outpos += nbytes;
    outsize -= nbytes;
}

void zmq::rdma_engine_t::activate_out ()
{
    //  We set POLLIN as completed write events are read from the completion
    //  queue and there is no POLLOUT event to indicate when we can send data.
    set_pollin (cq_handle);

    //  Speculative write: The assumption is that at the moment new message
    //  was sent by the user the queue-pair is probably available for writing.

    //  TODO: Implement the speculative write.
}

void zmq::rdma_engine_t::activate_in ()
{
    set_pollin (cq_handle);

    //  Speculative read.

    //  TODO: Implement the speculative read.
}

void zmq::rdma_engine_t::error ()
{
    zmq_assert (session);
    session->detach ();
    unplug ();
    delete this;
}

int zmq::rdma_engine_t::write (const void *data_, size_t size_)
{
    // TODO: Implement
    return 0;
}

int zmq::rdma_engine_t::read (void *data_, size_t size_)
{
    // TODO: Implement
    return 0;
}

//  Returns a reasonable size for the receive buffer depending on the receive
//  buffer option.

int zmq::rdma_engine_t::est_rx_buffer_size ()
{
    int size = def_rx_queue_depth * def_datagram_size;

    if (options.rcvbuf > 0) {
        //  Round the buffer to the datagram size.
        size = ((options.rcvbuf + def_datagram_size - 1) / def_datagram_size)
            * def_datagram_size;
    }

    return size;
}

//  Returns a reasonable depth for the receive queue depending on the
//  receive high watermark and receive buffer size. The number of pending
//  operations will also be checked against the amount supported by the HCA.

int zmq::rdma_engine_t::est_rx_queue_depth ()
{
    ibv_device_attr attr;
    int rc;
    int depth = std::min (est_rx_buffer_size () / def_datagram_size,
        options.rcvhwm);

    //  Retrieve the maximum number of WRs supported by the HCA.
    rc = ibv_query_device (id->verbs, &attr);
    posix_assert (rc);

    return std::min (depth, attr.max_qp_wr / 2);
}

//  Returns a reasonable depth for the transfer queue depending on the
//  send high watermark and the estimated send buffer size. The number of
//  pending operations will also be checked against the amount supported by the
//  HCA.

int zmq::rdma_engine_t::est_tx_queue_depth ()
{
    ibv_device_attr attr;
    int rc;
    int depth = std::min ((int) def_tx_queue_depth, options.sndhwm);

    //  Retrieve the maximum number of WRs supported by the HCA.
    rc = ibv_query_device (id->verbs, &attr);
    posix_assert (rc);

    return std::min (depth, attr.max_qp_wr / 2);
}

#endif //  ZMQ_HAVE_RDMA
