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

        rdma_engine_t (rdma_cm_id *id_, const options_t &options_,
                       bool active_);
        ~rdma_engine_t ();

        int init ();

        //  i_engine interface implementation.
        void plug (zmq::io_thread_t *io_thread_,
           zmq::session_base_t *session_);
        void unplug ();
        void terminate ();
        void activate_in ();
        void activate_out ();

        //  i_poll_events interface implementation.
        void in_event (fd_t fd_);

    private:

        //  Handle completion channel and completion queue events.
        void qp_event ();
        bool id_event ();

        //  Write data to the send buffer.
        void fill_snd_buffer ();

        //  Post as many send operations as possible.
        void post_send_wrs ();

        //  Function to handle network disconnections.
        void error ();

        //  Functions used to manipulate the circular send buffer.
        uint32_t snd_avail ();
        void update_snd_avail (uint32_t size_);
        uint32_t snd_pending ();
        void update_snd_pending (uint32_t size_);
        void update_snd_posted (uint32_t off_);
        uint32_t snd_wrs_avail ();

        //  Helper functions for sizing the connection parameters.
        int est_buffer_size (int def_qd_, int buf_size_);
        int est_queue_depth (int queue_depth_, int buf_size_);

        //  Internal constants
        enum {
            def_datagram_size = 1024, //  Default size of a datagram.
            def_rcv_queue_depth = 32,  //  Default depth of the RX queue.
            def_snd_queue_depth = 32   //  Default depth of the TX queue.
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

        //  Depth of the send and receive queues.
        int rcv_queue_depth;
        int snd_queue_depth;

        //  Receive buffer, size and associated memory region.
        size_t rcv_buffer_size;
        unsigned char *rcv_buffer;
        ibv_mr *rcv_mr;

        //  Send buffer, size and associated memory region.
        size_t snd_buffer_size;
        unsigned char *snd_buffer;
        ibv_mr *snd_mr;

        //  Offsets in the circular send buffer. They point respectively to
        //  the available space in the buffer (avail), to the data written by
        //  the encoder and awaiting to be sent (pending) and to the data that
        //  has already been posted on the queue-pair (posted).
        uint32_t snd_avail_off;
        uint32_t snd_pending_off;
        uint32_t snd_posted_off;

        //  Number of send work requested that have been posted / completed.
        uint32_t snd_wr_id;
        uint32_t snd_compl_wr_id;

        //  True if the object represents the active side of a connection.
        bool active_p;

        //  Queue-pair completion queue and RDMA CM ID event channel handles.
        handle_t qp_handle;
        handle_t id_handle;


        //  Data encoder and decoder.
        decoder_t decoder;
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
