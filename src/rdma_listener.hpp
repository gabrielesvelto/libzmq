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

#ifndef __ZMQ_RDMA_LISTENER_HPP_INCLUDED__
#define __ZMQ_RDMA_LISTENER_HPP_INCLUDED__

#include "precompiled.hpp"

#if defined ZMQ_HAVE_RDMA

#include "fd.hpp"
#include "own.hpp"
#include "stdint.hpp"
#include "io_object.hpp"
#include "tcp_address.hpp"

#include <rdma/rdma_cma.h>

namespace zmq
{

    class io_thread_t;
    class socket_base_t;

    class rdma_listener_t : public own_t, public io_object_t
    {
    public:

        rdma_listener_t (zmq::io_thread_t *io_thread_,
            zmq::socket_base_t *socket_, const options_t &options_);
        ~rdma_listener_t ();

        //  Set address to listen on.
        int set_address (const char *addr_);

    private:

        //  Handlers for incoming commands.
        void process_plug ();
        void process_term (int linger_);

        //  Handlers for I/O events.
        void in_event ();

        //  Accept the new connection. Returns the RDMA ID of the newly created
        //  connection. The function may return NULL if the connection attempt
        //  fails. After the accept () call the connection is not established
        //  yet, it will be established only after receiving an RDMA event of
        //  type RDMA_CM_EVENT_ESTABLISHED.
        rdma_cm_id *accept (const rdma_cm_event *event);

        //  Address to listen on, the RDMA connection manager uses plain IPv4
        //  or IPv6 address so we can re-use tcp_address_t for it.
        tcp_address_t address;

        //  RDMA event channel, this object contains a file descriptor which is
        //  used as the underlying system for notifying the user of messages
        //  arriving on the RDMA ID.
        rdma_event_channel *channel;

        //  Underlying RDMA connection manager ID.
        rdma_cm_id *id;

        //  Handle corresponding to the listening ID.
        handle_t handle;

        //  Socket the listerner belongs to.
        zmq::socket_base_t *socket;

        rdma_listener_t (const rdma_listener_t&);
        const rdma_listener_t &operator = (const rdma_listener_t&);
    };
}

#endif //  ZMQ_HAVE_RDMA

#endif //  __ZMQ_RDMA_LISTENER_HPP_INCLUDED__
