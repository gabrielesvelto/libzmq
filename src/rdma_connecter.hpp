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

#ifndef __RDMA_CONNECTER_HPP_INCLUDED__
#define __RDMA_CONNECTER_HPP_INCLUDED__

#include "precompiled.hpp"

#if defined ZMQ_HAVE_RDMA

#include <rdma/rdma_cma.h>

#include "address.hpp"
#include "fd.hpp"
#include "own.hpp"
#include "stdint.hpp"
#include "io_object.hpp"
#include "rdma_engine.hpp"

namespace zmq
{

    class io_thread_t;
    class session_base_t;

    class rdma_connecter_t : public own_t, public io_object_t
    {
    public:

        //  If 'delay' is true connecter first waits for a while, then starts
        //  connection process.
        rdma_connecter_t (zmq::io_thread_t *io_thread_,
            zmq::session_base_t *session_, const options_t &options_,
            const char *address_, bool delay_);
        ~rdma_connecter_t ();

    private:

        //  Handlers for incoming commands.
        void process_plug ();

        //  Handlers for I/O events.
        void in_event (fd_t fd_);
        void timer_event (handle_t handle_);

        //  Internal function to start the actual connection establishment.
        void start_connecting ();

        //  Internal function that creates the RDMA engine object and resolves
        //  the route to the other end-point.
        int resolve_route ();

        //  Internal function to start the RDMA engine object.
        void start_engine (rdma_engine_t *engine_);

        //  Internal function to add a reconnect timer
        void add_reconnect_timer();

        //  Internal function to return a reconnect backoff delay.
        //  Will modify the current_reconnect_ivl used for next call
        //  Returns the currently used interval
        int get_new_reconnect_ivl ();

        //  Set address to connect to.
        int set_address (const char *addr_);

        //  Creates the event channel and the RDMA connection manager ID.
        //  Returns -1 in case of error, 0 if the creation was successfull.
        int open ();

        //  Destroy the RDMA connection manager id and event channel.
        void close ();

        //  Address to connect to.
        tcp_address_t address;

        //  RDMA event channel, this object contains a file descriptor which is
        //  used as the underlying system for notifying the user of messages
        //  arriving on the RDMA ID.
        rdma_event_channel *channel;

        //  Underlying RDMA connection manager ID.
        rdma_cm_id *id;

        //  Handle corresponding to the listening socket.
        handle_t handle;

        //  If true, connecter is waiting a while before trying to connect.
        bool wait;

        //  Reference to the session we belong to.
        zmq::session_base_t *session;

        //  Current reconnect ivl, updated for backoff strategy
        int current_reconnect_ivl;

        //  Handle to reconnect timer, if active, NULL otherwise.
        handle_t reconnect_timer;

        rdma_connecter_t (const rdma_connecter_t&);
        const rdma_connecter_t &operator = (const rdma_connecter_t&);
    };

}

#endif //  ZMQ_HAVE_RDMA

#endif //  __ZMQ_RDMA_CONNECTER_HPP_INCLUDED__
