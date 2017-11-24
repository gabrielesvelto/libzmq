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

#ifndef __ZMQ_RDMA_ENGINE_HPP_INCLUDED__
#define __ZMQ_RDMA_ENGINE_HPP_INCLUDED__

#include "precompiled.hpp"

#if defined ZMQ_HAVE_RDMA

#include <stddef.h>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include "fd.hpp"
#include "i_engine.hpp"
#include "io_object.hpp"
#include "encoder.hpp"
#include "decoder.hpp"
#include "options.hpp"

namespace zmq
{

    class io_thread_t;
    class session_base_t;

    //  This engine handles InfiniBand verbs sockets, this covers both
    //  InfiniBand, iWARP and the RDMA over Converged Ethernet transports.

    class rdma_engine_t : public io_object_t, public i_engine
    {
    public:

        rdma_engine_t (rdma_cm_id *id_, const options_t &options_);
        ~rdma_engine_t ();

        //  i_engine interface implementation.
        void plug (zmq::io_thread_t *io_thread_,
           zmq::session_base_t *session_);
        void unplug ();
        void terminate ();
        void activate_in ();
        void activate_out ();

        //  i_poll_events interface implementation.
        void in_event ();
        void out_event ();

        //  Returns true if the object was correctly initialized, false if an
        //  error occurred during initialization
        bool initialized () { return initialized_p; }

    private:

        //  Function to handle network disconnections.
        void error ();

        //  Writes data to the queue-pair. Returns the number of bytes actually
        //  written (even zero is to be considered to be a success). In case
        //  of error or orderly shutdown by the other peer -1 is returned.
        int write (const void *data_, size_t size_);

        //  Writes data to the queue-pair (up to 'size' bytes). Returns the
        //  number of bytes actually read (even zero is to be considered to be
        //  a success). In case of error or orderly shutdown by the other
        //  peer -1 is returned.
        int read (void *data_, size_t size_);

        //  Helper functions for sizing the connection parameters.
        int est_rx_buffer_size ();
        int est_rx_queue_depth ();
        int est_tx_queue_depth ();

        // Internal constants
        enum {
            def_datagram_size = 1024, //  Default size of a datagram.
            def_rx_queue_depth = 32,  //  Default depth of the RX queue.
            def_tx_queue_depth = 32   //  Default depth of the TX queue.
        };

        //  Underlying RDMA ID.
        rdma_cm_id *id;

        //  Verbs protection domain.
        ibv_pd *pd;

        //  Completion channel, this will provide the fd for the poller.
        ibv_comp_channel *comp_channel;

        //  Completion queue associated with this connection.
        ibv_cq *cq;

        //  Queue-pair.
        ibv_qp *qp;

        //  Depth of the transfer and receive queues.
        int tx_queue_depth;
        int rx_queue_depth;

        //  Receive buffer, size and associated memory region.
        size_t rx_buffer_size;
        char *rx_buffer;
        ibv_mr *rx_mr;

        // True if the object was successfully initialized
        bool initialized_p;

        handle_t handle;

        unsigned char *inpos;
        size_t insize;
        decoder_t decoder;

        unsigned char *outpos;
        size_t outsize;
        encoder_t encoder;

        //  The session this engine is attached to.
        zmq::session_base_t *session;

        //  Detached transient session.
        zmq::session_base_t *leftover_session;

        options_t options;

        bool plugged;

        rdma_engine_t (const rdma_engine_t&);
        const rdma_engine_t &operator = (const rdma_engine_t&);
    };

}

#endif //  ZMQ_HAVE_RDMA

#endif // __ZMQ_RDMA_ENGINE_HPP_INCLUDED__
