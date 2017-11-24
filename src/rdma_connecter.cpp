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

#if !defined ZMQ_HAVE_WINDOWS
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#endif

#include <new>
#include <string>

#include "rdma_connecter.hpp"
#include "rdma_engine.hpp"
#include "io_thread.hpp"
#include "platform.hpp"
#include "random.hpp"
#include "err.hpp"
#include "ip.hpp"

zmq::rdma_connecter_t::rdma_connecter_t (class io_thread_t *io_thread_,
      class session_base_t *session_, const options_t &options_,
      const char *address_, bool wait_) :
    own_t (io_thread_, options_),
    io_object_t (io_thread_),
    id (NULL),
    wait (wait_),
    session (session_),
    current_reconnect_ivl (options.reconnect_ivl),
    reconnect_timer (NULL)
{
    //  TODO: set_addess should be called separately, so that the error
    //  can be propagated.
    int rc = set_address (address_);
    errno_assert (rc == 0);
}

zmq::rdma_connecter_t::~rdma_connecter_t ()
{
    if (wait) {
        zmq_assert (reconnect_timer);
        rm_timer (reconnect_timer);
        reconnect_timer = NULL;
    }

    if (handle)
        rm_fd (handle);

    if (id != NULL)
        close ();
}

void zmq::rdma_connecter_t::process_plug ()
{
    if (wait)
        add_reconnect_timer();
    else
        start_connecting ();
}

//  TODO: Deal with disconnections and other errors, two messages should be
//  dealt with, RDMA_CM_EVENT_DISCONNECTED and RDMA_CM_EVENT_UNREACHABLE.

void zmq::rdma_connecter_t::in_event (fd_t fd_)
{
    rdma_conn_param conn_param;
    rdma_cm_event *event;
    int err = 0; //  If non-zero something went wrong while handling an event.
    int rc = rdma_get_cm_event (channel, &event);
    errno_assert (rc == 0);

    switch (event->event) {

    case RDMA_CM_EVENT_ADDR_RESOLVED:
        err = rdma_resolve_route (id, sm_timeout);
        break;

    case RDMA_CM_EVENT_ROUTE_RESOLVED:
        memset (&conn_param, 0, sizeof(conn_param));
        conn_param.retry_count = retry_count;
        conn_param.rnr_retry_count = rnr_retry_count;
        err = rdma_connect (id, &conn_param);
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        //  We established the connection, start the engine object.
        start_engine ();
        break;

    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_REJECTED:
        //  Wait and try resuming the connection later.
        err = -1;
        break;

    default:
        ; //  Ignore all other events.
    }

    rc = rdma_ack_cm_event (event);
    errno_assert (rc == 0);

    if (err) {
        //  Handle the error condition by attempt to reconnect.
        rm_fd (handle);
        handle = NULL;
        close ();
        wait = true;
        add_reconnect_timer();
    }
}

void zmq::rdma_connecter_t::timer_event (handle_t handle_)
{
    zmq_assert (handle_ == reconnect_timer);
    reconnect_timer = NULL;
    wait = false;
    start_connecting ();
}

void zmq::rdma_connecter_t::start_connecting ()
{
    int rc = open ();

    if (rc == 0) {
        zmq_assert (!handle);
        handle = add_fd (channel->fd);
        set_pollin (handle);

        //  Resolve the end-point address.
        rc = rdma_resolve_addr (id, NULL, address.addr (), sm_timeout);

        if (rc == 0)
            return; //  Everything went fine.

        //  Address resolution failed, remove the handle and retry.
        rm_fd (handle);
        handle = NULL;
    }

    //  Handle the error condition by attempt to reconnect.
    close ();
    wait = true;
    add_reconnect_timer();
}

void zmq::rdma_connecter_t::start_engine ()
{
    //  Create the engine object for this connection.
    rdma_engine_t *engine = new (std::nothrow) rdma_engine_t (id, options,
        true);
    alloc_assert (engine);

    //  Remove the event channel from the polling set and remove the RDMA
    //  connection manager ID from the connecter, it will be passed together
    //  with the associated event channel to the engine.
    rm_fd (handle);
    handle = NULL;
    id = NULL;
    channel = NULL;

    //  Attach the engine to the corresponding session object.
    send_attach (session, engine);

    //  Shut the connecter down.
    terminate ();
}

void zmq::rdma_connecter_t::add_reconnect_timer ()
{
    zmq_assert (!reconnect_timer);
    reconnect_timer = add_timer (get_new_reconnect_ivl());
}

int zmq::rdma_connecter_t::get_new_reconnect_ivl ()
{
    //  The new interval is the current interval + random value.
    int this_interval = current_reconnect_ivl +
        (generate_random () % options.reconnect_ivl);

    //  Only change the current reconnect interval  if the maximum reconnect
    //  interval was set and if it's larger than the reconnect interval.
    if (options.reconnect_ivl_max > 0 &&
        options.reconnect_ivl_max > options.reconnect_ivl) {

        //  Calculate the next interval
        current_reconnect_ivl = current_reconnect_ivl * 2;
        if(current_reconnect_ivl >= options.reconnect_ivl_max) {
            current_reconnect_ivl = options.reconnect_ivl_max;
        }
    }
    return this_interval;
}

int zmq::rdma_connecter_t::set_address (const char *addr_)
{
    return address.resolve (addr_, false, options.ipv4only ? true : false);
}

int zmq::rdma_connecter_t::open ()
{
    zmq_assert ((channel == NULL) && (id == NULL));

    //  Create the event channel.
    channel = rdma_create_event_channel ();
    if (channel == NULL)
        return -1;

    //  Create the RDMA connection manager ID.
    int rc = rdma_create_id (channel, &id, NULL, RDMA_PS_TCP);
    if (rc != 0) {
        rdma_destroy_event_channel (channel);
        channel = NULL;
        return -1;
    }

    return 0;
}

void zmq::rdma_connecter_t::close ()
{
    int rc;

    zmq_assert ((channel != NULL) && (id != NULL));

    rc = rdma_destroy_id (id);
    errno_assert (rc == 0);
    id = NULL;

    rdma_destroy_event_channel (channel);
    channel = NULL;
}

#endif //  ZMQ_HAVE_RDMA
