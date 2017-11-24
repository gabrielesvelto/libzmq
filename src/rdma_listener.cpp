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

#include "precompiled.hpp"

#if defined ZMQ_HAVE_RDMA

#include <new>

#include <string.h>

#include "rdma_engine.hpp"
#include "rdma_listener.hpp"
#include "io_thread.hpp"
#include "session_base.hpp"
#include "config.hpp"
#include "err.hpp"
#include "ip.hpp"

#if !defined ZMQ_HAVE_WINDOWS
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#endif

#include <rdma/rdma_cma.h>

zmq::rdma_listener_t::rdma_listener_t (io_thread_t *io_thread_,
      socket_base_t *socket_, const options_t &options_) :
    own_t (io_thread_, options_),
    io_object_t (io_thread_),
    channel (NULL),
    id (NULL),
    socket (socket_)
{
}

zmq::rdma_listener_t::~rdma_listener_t ()
{
    int rc;

    if (id) {
        rc = rdma_destroy_id (id);
        errno_assert (rc == 0);
    }

    if (channel)
        rdma_destroy_event_channel (channel);
}

//  Add the event channel's file descriptor to the poller's fd set.

void zmq::rdma_listener_t::process_plug ()
{
    //  Start polling for incoming connections.
    handle = add_fd (channel->fd);
    set_pollin (handle);
}

//  Same as tcp_listener_t

void zmq::rdma_listener_t::process_term (int linger_)
{
    rm_fd (handle);
    own_t::process_term (linger_);
}

void zmq::rdma_listener_t::in_event (fd_t fd_)
{
    rdma_cm_event *event = NULL;
    rdma_engine_t *engine;
    io_thread_t *io_thread;
    session_base_t *session;
    int rc;

    rc = rdma_get_cm_event (channel, &event);
    errno_assert (rc == 0);

    switch (event->event) {

    case RDMA_CM_EVENT_CONNECT_REQUEST:
        accept (event);
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        //  Choose I/O thread to run connecter in. Given that we are already
        //  running in an I/O thread, there must be at least one available.
        io_thread = choose_io_thread (options.affinity);
        zmq_assert (io_thread);

        //  Retreive the engine from the RDMA connection manager ID.
        engine = (rdma_engine_t *) event->id->context;

        //  Create and launch a session object.
        session = session_base_t::create (io_thread, false, socket, options,
            NULL, NULL);
        errno_assert (session);
        session->inc_seqnum ();
        launch_child (session);
        send_attach (session, engine, false);
        break;

    case RDMA_CM_EVENT_DISCONNECTED:
        //  One of the IDs we created has been disconnected.
        engine = (rdma_engine_t *) event->id->context;

        //  FIXME: This cannot be done in this context, the engine object
        //  itself should call the unplug () method.
        engine->unplug ();
        delete engine;
        break;

    default:
        ; //  Ignore all other events.

    }

    rc = rdma_ack_cm_event (event);
    errno_assert (rc == 0);
}

//  Create the RDMA ID and the associated event channel that will be returned
//  for use by the poller, bind the ID and listen on it.

int zmq::rdma_listener_t::set_address (const char *addr_)
{
    //  Convert the textual address into address structure.
    int rc = address.resolve (addr_, true, options.ipv6);
    if (rc != 0)
        return -1;

    //  Create an event channel for receiving RDMA connection manager messages.
    channel = rdma_create_event_channel ();
    if (!channel)
        return -1;

    // Create the RDMA CM ID that will be used to listen on.
    rc = rdma_create_id (channel, &id, NULL, RDMA_PS_TCP);
    if (rc != 0)
        return -1;

#if ZMQ_HAVE_DECL_RDMA_OPTION_ID_REUSEADDR
    //  Allow reusing of the address, this is not always presented in the
    //  headers so we compile it conditionally
    int flag = 1;

    rc = rdma_set_option (id, RDMA_OPTION_ID, RDMA_OPTION_ID_REUSEADDR, &flag,
                          sizeof (int));
    errno_assert (rc == 0);
#endif // ZMQ_HAVE_DECL_RDMA_OPTION_ID_REUSEADDR

    //  Bind the RDMA ID to the network interface and port.
    rc = rdma_bind_addr (id, const_cast<sockaddr*>(address.addr ()));
    if (rc != 0)
        return -1;

    //  Listen for incomming connections.
    rc = rdma_listen (id, options.backlog);
    if (rc != 0)
        return -1;

    return 0;
}

void zmq::rdma_listener_t::accept (const rdma_cm_event *event_)
{
    rdma_conn_param conn_param;
    rdma_cm_id *id = event_->id;
    int rc;

    rdma_engine_t *engine = new (std::nothrow) rdma_engine_t (id, options,
        false);
    alloc_assert (engine);
    rc = engine->init ();

    if (rc) {
        //  We failed to create an object, refuse the connection.
        delete engine;
        rc = rdma_reject (id, NULL, 0);
        errno_assert (rc == 0);
        return;
    }

    //  Store the newly created engine in the RDMA ID context pointer, we will
    //  read the engine from that field when the connection is established.
    id->context = (void *) engine;

    //  Accept the connection.
    memset (&conn_param, 0, sizeof(rdma_conn_param));
    conn_param.retry_count = rdma_retry_count;
    conn_param.rnr_retry_count = rdma_rnr_retry_count;
    rc = rdma_accept (id, &conn_param);
    errno_assert (rc == 0);
}

#endif //  ZMQ_HAVE_RDMA
