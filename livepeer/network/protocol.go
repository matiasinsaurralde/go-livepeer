// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package network

/*
bzz implements the swarm wire protocol [bzz] (sister of eth and shh)
the protocol instance is launched on each peer by the network layer if the
bzz protocol handler is registered on the p2p server.

The bzz protocol component speaks the bzz protocol
* handle the protocol handshake
* register peers in the KΛÐΞMLIΛ table via the hive logistic manager
* dispatch to hive for handling the DHT logic
* encode and decode requests for storage and retrieval
* handle sync protocol messages via the syncer
* talks the SWAP payment protocol (swap accounting is done within NetStore)
*/

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/contracts/chequebook"
	"github.com/ethereum/go-ethereum/errs"
	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	bzzswap "github.com/ethereum/go-ethereum/swarm/services/swap"
	"github.com/ethereum/go-ethereum/swarm/services/swap/swap"
	"github.com/livepeer/go-livepeer/livepeer/storage"
	"github.com/livepeer/go-livepeer/livepeer/storage/streaming"
	lpmsIo "github.com/livepeer/go-livepeer/lpms/io"
	streamingVizClient "github.com/livepeer/streamingviz/client"
)

const (
	Version            = 0
	ProtocolLength     = uint64(12)
	ProtocolMaxMsgSize = 10 * 1024 * 1024
	NetworkId          = 326326
)

const (
	ErrMsgTooLarge = iota
	ErrDecode
	ErrInvalidMsgCode
	ErrVersionMismatch
	ErrNetworkIdMismatch
	ErrNoStatusMsg
	ErrExtraStatusMsg
	ErrSwap
	ErrSync
	ErrUnwanted
	ErrTranscode
)

var errorToString = map[int]string{
	ErrMsgTooLarge:       "Message too long",
	ErrDecode:            "Invalid message",
	ErrInvalidMsgCode:    "Invalid message code",
	ErrVersionMismatch:   "Protocol version mismatch",
	ErrNetworkIdMismatch: "NetworkId mismatch",
	ErrNoStatusMsg:       "No status message",
	ErrExtraStatusMsg:    "Extra status message",
	ErrSwap:              "SWAP error",
	ErrSync:              "Sync error",
	ErrUnwanted:          "Unwanted peer",
}

// bzz represents the swarm wire protocol
// an instance is running on each peer
type bzz struct {
	selfID     discover.NodeID      // peer's node id used in peer advertising in handshake
	key        storage.Key          // baseaddress as storage.Key
	storage    StorageHandler       // handler storage/retrieval related requests coming via the bzz wire protocol
	hive       *Hive                // the logistic manager, peerPool, routing service and peer handler
	streamer   *streaming.Streamer  // broker for video streaming, provider of video channels
	streamDB   *StreamDB            // keeps track of the downstream peers requesting a stream in the network layer
	forwarder  *storage.CloudStore  // The forwarder
	dbAccess   *DbAccess            // access to db storage counter and iterator for syncing
	requestDb  *storage.LDBDatabase // db to persist backlog of deliveries to aid syncing
	remoteAddr *peerAddr            // remote peers address
	peer       *p2p.Peer            // the p2p peer object
	rw         p2p.MsgReadWriter    // messageReadWriter to send messages to
	errors     *errs.Errors         // errors table
	backend    chequebook.Backend
	lastActive time.Time
	NetworkId  uint64

	swap        *swap.Swap          // swap instance for the peer connection
	swapParams  *bzzswap.SwapParams // swap settings both local and remote
	swapEnabled bool                // flag to enable SWAP (will be set via Caps in handshake)
	syncEnabled bool                // flag to enable SYNC (will be set via Caps in handshake)
	syncer      *syncer             // syncer instance for the peer connection
	syncParams  *SyncParams         // syncer params
	syncState   *syncState          // outgoing syncronisation state (contains reference to remote peers db counter)
	viz         *streamingVizClient.Client
}

// interface type for handler of storage/retrieval related requests coming
// via the bzz wire protocol
// messages: UnsyncedKeys, DeliveryRequest, StoreRequest, RetrieveRequest
type StorageHandler interface {
	HandleUnsyncedKeysMsg(req *unsyncedKeysMsgData, p *peer) error
	HandleDeliveryRequestMsg(req *deliveryRequestMsgData, p *peer) error
	HandleStoreRequestMsg(req *storeRequestMsgData, p *peer)
	HandleRetrieveRequestMsg(req *retrieveRequestMsgData, p *peer)
}

/*
main entrypoint, wrappers starting a server that will run the bzz protocol
use this constructor to attach the protocol ("class") to server caps
This is done by node.Node#Register(func(node.ServiceContext) (Service, error))
Service implements Protocols() which is an array of protocol constructors
at node startup the protocols are initialised
the Dev p2p layer then calls Run(p *p2p.Peer, rw p2p.MsgReadWriter) error
on each peer connection
The Run function of the Bzz protocol class creates a bzz instance
which will represent the peer for the swarm hive and all peer-aware components
*/
func Bzz(cloud StorageHandler, backend chequebook.Backend, hive *Hive, dbaccess *DbAccess, sp *bzzswap.SwapParams, sy *SyncParams, networkId uint64, streamer *streaming.Streamer, streamDB *StreamDB, forwarder *storage.CloudStore, viz *streamingVizClient.Client) (p2p.Protocol, error) {

	// a single global request db is created for all peer connections
	// this is to persist delivery backlog and aid syncronisation
	requestDb, err := storage.NewLDBDatabase(sy.RequestDbPath)
	if err != nil {
		return p2p.Protocol{}, fmt.Errorf("error setting up request db: %v", err)
	}
	if networkId == 0 {
		networkId = NetworkId
	}
	return p2p.Protocol{
		Name:    "bzz",
		Version: Version,
		Length:  ProtocolLength,
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			return run(requestDb, cloud, backend, hive, dbaccess, sp, sy, networkId, p, rw, streamer, streamDB, forwarder, viz)
		},
	}, nil
}

/*
the main protocol loop that
 * does the handshake by exchanging statusMsg
 * if peer is valid and accepted, registers with the hive
 * then enters into a forever loop handling incoming messages
 * storage and retrieval related queries coming via bzz are dispatched to StorageHandler
 * peer-related messages are dispatched to the hive
 * payment related messages are relayed to SWAP service
 * on disconnect, unregister the peer in the hive (note RemovePeer in the post-disconnect hook)
 * whenever the loop terminates, the peer will disconnect with Subprotocol error
 * whenever handlers return an error the loop terminates
*/
func run(requestDb *storage.LDBDatabase, depo StorageHandler, backend chequebook.Backend, hive *Hive, dbaccess *DbAccess, sp *bzzswap.SwapParams, sy *SyncParams, networkId uint64, p *p2p.Peer, rw p2p.MsgReadWriter, streamer *streaming.Streamer, streamDB *StreamDB, forwarder *storage.CloudStore, viz *streamingVizClient.Client) (err error) {

	self := &bzz{
		storage:   depo,
		backend:   backend,
		hive:      hive,
		dbAccess:  dbaccess,
		requestDb: requestDb,
		peer:      p,
		rw:        rw,
		errors: &errs.Errors{
			Package: "BZZ",
			Errors:  errorToString,
		},
		swapParams:  sp,
		syncParams:  sy,
		swapEnabled: hive.swapEnabled,
		syncEnabled: true,
		NetworkId:   networkId,
		streamer:    streamer,
		streamDB:    streamDB,
		forwarder:   forwarder,
		viz:         viz,
	}

	// handle handshake
	err = self.handleStatus()
	if err != nil {
		return err
	}
	defer func() {
		// if the handler loop exits, the peer is disconnecting
		// deregister the peer in the hive
		self.hive.removePeer(&peer{bzz: self})
		if self.syncer != nil {
			self.syncer.stop() // quits request db and delivery loops, save requests
		}
		if self.swap != nil {
			self.swap.Stop() // quits chequebox autocash etc
		}
	}()

	// the main forever loop that handles incoming requests
	for {
		if self.hive.blockRead {
			glog.V(logger.Warn).Infof("Cannot read network")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err = self.handle()
		if err != nil {
			return
		}
	}
}

// TODO: may need to implement protocol drop only? don't want to kick off the peer
// if they are useful for other protocols
func (self *bzz) Drop() {
	self.peer.Disconnect(p2p.DiscSubprotocolError)
}

// one cycle of the main forever loop that handles and dispatches incoming messages
func (self *bzz) handle() error {
	msg, err := self.rw.ReadMsg()
	// fmt.Println("Got msg in protocol: ", msg)
	glog.V(logger.Debug).Infof("<- %v", msg)
	if err != nil {
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return self.protoError(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	// make sure that the payload has been fully consumed
	defer msg.Discard()

	switch msg.Code {

	case statusMsg:
		// no extra status message allowed. The one needed already handled by
		// handleStatus
		glog.V(logger.Debug).Infof("Status message: %v", msg)
		return self.protoError(ErrExtraStatusMsg, "")

	case streamRequestMsg:
		var req streamRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return err
		}

		originNode := req.OriginNode
		streamID := req.StreamID
		concatedStreamID := streaming.MakeStreamID(originNode, streamID)

		// Get the stream object out of the streamer
		stream, err := self.streamer.GetStream(originNode, streamID)
		if err != nil {
			// Got an error, return
			return err
		}

		if req.Id == streaming.RequestStreamMsgID {
			if stream == nil {
				// Don't have this stream yet. Subscribe and request it.
				stream, _ = self.streamer.SubscribeToStream(string(concatedStreamID))
				(*self.forwarder).Stream(string(concatedStreamID), self.remoteAddr.Addr)

				// Log the relay
				self.viz.LogRelay(string(concatedStreamID))
			}
			// Aready subscribed to this stream. Add this peer to the downstream requesters
			self.streamDB.AddDownstreamPeer(concatedStreamID, &peer{bzz: self})
			glog.V(logger.Info).Infof("Registering %v as a downstream requester for stream %v", self.remoteAddr.Addr, stream.ID)

			if len(self.streamDB.DownstreamRequesters[concatedStreamID]) == 1 {
				// First peer, kick off the sync thread
				go self.syncStreamToDownstreamRequesters(stream)
			}

		} else {
			// In this case req.Id == DeliverStreamMsgID || EOFStreamMsgID, so there is data in the req.SData field
			chunk := streaming.ByteArrInVideoChunk(req.SData)

			// Update upstream peers so we can unsubscribe later if necessary
			if self.streamDB.ContainsUpstreamPeer(concatedStreamID, &peer{bzz: self}) == false {
				self.streamDB.AddUpstreamPeer(concatedStreamID, &peer{bzz: self})
			}

			downstreamRequesters := self.streamDB.DownstreamRequesters[concatedStreamID]
			if len(downstreamRequesters) > 0 {
				// Write data to the Src channel of the stream so that it can be
				// propagated downstream
				stream.PutToSrcVideoChan(&chunk)
			}

			//Play to local video consumer
			if chunk.Seq%100 == 0 {
				fmt.Printf("video seq: %d\n", chunk.Seq)
			}

			stream.PutToDstVideoChan(&chunk)

			// Close the source channel and delete the stream if this was an EOF msg
			if req.Id == streaming.EOFStreamMsgID {
				close(stream.SrcVideoChan)
				self.streamer.DeleteStream(concatedStreamID)
			}
		}

	case streamUnsubscribeMsg:
		var req streamUnsubscribeMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}

		originNode := req.OriginNode
		streamID := req.StreamID
		concatedStreamID := streaming.MakeStreamID(originNode, streamID)

		// Remove this streamID from the downstream peers.
		self.streamDB.DeleteDownstreamPeer(concatedStreamID, &peer{bzz: self})

		//Unsubscribe ourselves if we aren't playing and have no downstream peers.
		msg := &streamUnsubscribeMsgData{
			OriginNode: originNode,
			StreamID:   streamID,
			from:       &peer{bzz: self},
		}

		for _, val := range self.streamDB.UpstreamProviders[concatedStreamID] {
			val.streamUnsubscribe(msg)
		}

	case storeRequestMsg:
		// store requests are dispatched to netStore
		var req storeRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		if len(req.SData) < 9 {
			return self.protoError(ErrDecode, "<- %v: Data too short (%v)", msg)
		}

		// last Active time is set only when receiving chunks
		self.lastActive = time.Now()
		glog.V(logger.Detail).Infof("incoming store request: %s", req.String())
		// swap accounting is done within forwarding
		self.storage.HandleStoreRequestMsg(&req, &peer{bzz: self})

	case retrieveRequestMsg:
		// retrieve Requests are dispatched to netStore
		var req retrieveRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		// fmt.Println("Protocol got retrieveRequestMsg")
		req.from = &peer{bzz: self}
		// if request is lookup and not to be delivered
		if req.isLookup() {
			glog.V(logger.Detail).Infof("self lookup for %v: responding with peers only...", req.from)
		} else if req.Key == nil {
			return self.protoError(ErrDecode, "protocol handler: req.Key == nil || req.Timeout == nil")
		} else {
			// swap accounting is done within netStore
			self.storage.HandleRetrieveRequestMsg(&req, &peer{bzz: self})
		}
		// direct response with peers, TODO: sort this out
		self.hive.peers(&req)

	case transcodeRequestMsg:
		var req transcodeRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		req.from = &peer{bzz: self}

		fmt.Println("Got Transcode Request: ", req)
		key := req.TranscodeID.Bytes()
		glog.V(logger.Info).Infof("Requesting a peer with transcodeID: %x", req.TranscodeID)

		// Note this means the routing won't necessarily be routed to the absolute closes node in the network,
		// since the knowledge of the local node can be constrained.  However, for now, a local optimum is enough
		// to get the job done - since all we need is a single node that will do the transcoding work.
		peers := self.hive.getPeersCloserThanSelf(key, 1)

		if len(peers) == 1 {
			//Remember the upstream requester, forward to downstream peer
			glog.V(logger.Info).Infof("Peer we got is: %v", peers[0].Addr())
			fmt.Println("Forwarding to the closer node: ", peers[0].Addr())
			self.streamDB.AddUpstreamTranscodeRequester(streaming.MakeStreamID(req.OriginNode, req.OriginStreamID), &peer{bzz: self})
			peers[0].transcode(&req)
		} else if len(peers) == 0 {
			//You ARE the transcoder!
			fmt.Println("I AM the transcoder.")
			from := &peer{bzz: self}
			transcodedVidChan := make(chan *streaming.VideoChunk, 10) //This channel needs to be closed at some point.  When is transcoding done?

			originalStreamID := streaming.MakeStreamID(req.OriginNode, req.OriginStreamID)
			//Subscribe to the original video
			originalStream, err := self.streamer.GetStreamByStreamID(originalStreamID)
			if originalStream == nil {
				originalStream, err = self.streamer.SubscribeToStream(string(originalStreamID))
				if err != nil {
					glog.V(logger.Error).Infof("Error subscribing to stream %v", err)
					return self.protoError(ErrTranscode, "Error subscribing to stream %v", err)
				}
				//Send subscribe request
				(*self.forwarder).Stream(string(originalStreamID), self.remoteAddr.Addr)
			}

			transcodedStream, err := self.streamer.AddNewStream()
			err = lpmsIo.Transcode(originalStream.DstVideoChan, transcodedVidChan, transcodedStream.ID, req.Formats[0], req.Bitrates[0], req.CodecIn, req.CodecOut[0], originalStream.CloseChan)
			go lpmsIo.CopyChannelToChannel(transcodedVidChan, transcodedStream.SrcVideoChan)

			//TODO: Need to spin up a Go Routine to monitor HLS playlist - if the past 10 are the same, close the transcodeStream

			if err != nil {
				self.streamer.DeleteStream(transcodedStream.ID)
				ack := &transcodeAckMsgData{
					OriginNode:     req.OriginNode,
					OriginStreamID: req.OriginStreamID,
				}
				glog.V(logger.Error).Infof("Got error during transcoding, sending empty ack.  %s", err)
				from.transcodeAck(ack)
			} else {
				transcodedID := transcodedStream.ID
				tsd := transcodedStreamData{
					StreamID: string(transcodedID),
					Format:   req.Formats[0],
					Bitrate:  req.Bitrates[0],
					CodecIn:  req.CodecIn,
					CodecOut: req.CodecOut[0],
				}
				ack := &transcodeAckMsgData{
					OriginNode:     req.OriginNode,
					OriginStreamID: req.OriginStreamID,
					NewStreamIDs:   []transcodedStreamData{tsd},
				}
				glog.V(logger.Info).Infof("Sending Ack...")
				from.transcodeAck(ack)

			}
		} else {
			return self.protoError(ErrTranscode, "Error - Got %d downstream transcoders from the swarm.", len(peers))
		}

	case transcodeAckMsg:
		var req transcodeAckMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		//Check local map to see if you need to pass it back to upstream requester
		upstreamPeer := self.streamDB.UpstreamTranscodeRequesters[streaming.MakeStreamID(req.OriginNode, req.OriginStreamID)]
		// for k, _ := range self.streamDB.UpstreamTranscodeRequesters {
		// 	fmt.Println("Ack db key: ", k)
		// }
		// fmt.Println("Peer for Id: ", upstreamPeer, streaming.MakeStreamID(req.OriginNode, req.OriginStreamID))
		if upstreamPeer != nil {
			glog.V(logger.Info).Infof("Forwarding Transcode Ack to upstream peer")
			upstreamPeer.transcodeAck(&req)
		} else {
			glog.V(logger.Info).Infof("Got Transcode Ack: ", req)
			if len(req.NewStreamIDs) == 0 {
				//Transcode failed.  Need to:
				//1. Send a EOF packet to close the RTMP stream to current requester
				//2. Request for another transcoder (Maybe just call forwarder.Transcode() again?)
			} else {
				for _, newID := range req.NewStreamIDs {
					self.streamDB.AddTranscodedStream(streaming.MakeStreamID(req.OriginNode, req.OriginStreamID), newID)
					glog.V(logger.Info).Infof("Transcoded Stream: ", newID)
				}
			}
		}

	case peersMsg:
		// response to lookups and immediate response to retrieve requests
		// dispatches new peer data to the hive that adds them to KADDB
		var req peersMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		req.from = &peer{bzz: self}
		glog.V(logger.Detail).Infof("<- peer addresses: %v", req)
		self.hive.HandlePeersMsg(&req, &peer{bzz: self})

	case syncRequestMsg:
		var req syncRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		glog.V(logger.Debug).Infof("<- sync request: %v", req)
		self.lastActive = time.Now()
		self.sync(req.SyncState)

	case unsyncedKeysMsg:
		// coming from parent node offering
		var req unsyncedKeysMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}
		glog.V(logger.Debug).Infof("<- unsynced keys : %s", req.String())
		err := self.storage.HandleUnsyncedKeysMsg(&req, &peer{bzz: self})
		self.lastActive = time.Now()
		if err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}

	case deliveryRequestMsg:
		// response to syncKeysMsg hashes filtered not existing in db
		// also relays the last synced state to the source
		var req deliveryRequestMsgData
		if err := msg.Decode(&req); err != nil {
			return self.protoError(ErrDecode, "<-msg %v: %v", msg, err)
		}
		glog.V(logger.Debug).Infof("<- delivery request: %s", req.String())
		err := self.storage.HandleDeliveryRequestMsg(&req, &peer{bzz: self})
		self.lastActive = time.Now()
		if err != nil {
			return self.protoError(ErrDecode, "<- %v: %v", msg, err)
		}

	case paymentMsg:
		// swap protocol message for payment, Units paid for, Cheque paid with
		if self.swapEnabled {
			var req paymentMsgData
			if err := msg.Decode(&req); err != nil {
				return self.protoError(ErrDecode, "<- %v: %v", msg, err)
			}
			glog.V(logger.Debug).Infof("<- payment: %s", req.String())
			self.swap.Receive(int(req.Units), req.Promise)
		}

	default:
		// no other message is allowed
		return self.protoError(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (self *bzz) handleStatus() (err error) {

	handshake := &statusMsgData{
		Version:   uint64(Version),
		ID:        "honey",
		Addr:      self.selfAddr(),
		NetworkId: uint64(self.NetworkId),
		Swap: &bzzswap.SwapProfile{
			Profile:    self.swapParams.Profile,
			PayProfile: self.swapParams.PayProfile,
		},
	}

	err = p2p.Send(self.rw, statusMsg, handshake)
	if err != nil {
		self.protoError(ErrNoStatusMsg, err.Error())
	}

	// read and handle remote status
	var msg p2p.Msg
	msg, err = self.rw.ReadMsg()
	if err != nil {
		return err
	}

	if msg.Code != statusMsg {
		self.protoError(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, statusMsg)
	}

	if msg.Size > ProtocolMaxMsgSize {
		return self.protoError(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}

	var status statusMsgData
	if err := msg.Decode(&status); err != nil {
		return self.protoError(ErrDecode, " %v: %v", msg, err)
	}

	if status.NetworkId != self.NetworkId {
		return self.protoError(ErrNetworkIdMismatch, "%d (!= %d)", status.NetworkId, self.NetworkId)
	}

	if Version != status.Version {
		return self.protoError(ErrVersionMismatch, "%d (!= %d)", status.Version, Version)
	}

	self.remoteAddr = self.peerAddr(status.Addr)
	glog.V(logger.Detail).Infof("self: advertised IP: %v, peer advertised: %v, local address: %v\npeer: advertised IP: %v, remote address: %v\n", self.selfAddr(), self.remoteAddr, self.peer.LocalAddr(), status.Addr.IP, self.peer.RemoteAddr())

	if self.swapEnabled {
		// set remote profile for accounting
		self.swap, err = bzzswap.NewSwap(self.swapParams, status.Swap, self.backend, self)
		if err != nil {
			return self.protoError(ErrSwap, "%v", err)
		}
	}

	glog.V(logger.Info).Infof("Peer %08x is capable (%d/%d)", self.remoteAddr.Addr[:4], status.Version, status.NetworkId)
	err = self.hive.addPeer(&peer{bzz: self})
	if err != nil {
		return self.protoError(ErrUnwanted, "%v", err)
	}

	// hive sets syncstate so sync should start after node added
	glog.V(logger.Info).Infof("syncronisation request sent with %v", self.syncState)
	self.syncRequest()

	return nil
}

func (self *bzz) sync(state *syncState) error {
	// syncer setup
	if self.syncer != nil {
		return self.protoError(ErrSync, "sync request can only be sent once")
	}

	cnt := self.dbAccess.counter()
	remoteaddr := self.remoteAddr.Addr
	start, stop := self.hive.kad.KeyRange(remoteaddr)

	// an explicitly received nil syncstate disables syncronisation
	if state == nil {
		self.syncEnabled = false
		glog.V(logger.Warn).Infof("syncronisation disabled for peer %v", self)
		state = &syncState{DbSyncState: &storage.DbSyncState{}, Synced: true}
	} else {
		state.synced = make(chan bool)
		state.SessionAt = cnt
		if storage.IsZeroKey(state.Stop) && state.Synced {
			state.Start = storage.Key(start[:])
			state.Stop = storage.Key(stop[:])
		}
		glog.V(logger.Debug).Infof("syncronisation requested by peer %v at state %v", self, state)
	}
	var err error
	self.syncer, err = newSyncer(
		self.requestDb,
		storage.Key(remoteaddr[:]),
		self.dbAccess,
		self.unsyncedKeys, self.store,
		self.syncParams, state, func() bool { return self.syncEnabled },
	)
	if err != nil {
		return self.protoError(ErrSync, "%v", err)
	}
	glog.V(logger.Detail).Infof("syncer set for peer %v", self)
	return nil
}

func (self *bzz) String() string {
	return self.remoteAddr.String()
}

func (self *bzz) syncStreamToDownstreamRequesters(stream *streaming.Stream) {
	originNode, streamID := stream.ID.SplitComponents()
	for videoChunk := range stream.SrcVideoChan {

		msg := &streamRequestMsgData{
			OriginNode: originNode,
			StreamID:   streamID,
			SData:      streaming.VideoChunkToByteArr(*videoChunk),
			Id:         streaming.DeliverStreamMsgID,
		}

		for _, peer := range self.streamDB.DownstreamRequesters[stream.ID] {

			// Stream this to the requestor
			err := peer.stream(msg)
			if err != nil {
				glog.V(logger.Error).Infof("Error sending stream to requestor: %s\n", err)
				return
			}
		}
	}
}

// repair reported address if IP missing
func (self *bzz) peerAddr(base *peerAddr) *peerAddr {
	if base.IP.IsUnspecified() {
		host, _, _ := net.SplitHostPort(self.peer.RemoteAddr().String())
		base.IP = net.ParseIP(host)
	}
	return base
}

// returns self advertised node connection info (listening address w enodes)
// IP will get repaired on the other end if missing
// or resolved via ID by discovery at dialout
func (self *bzz) selfAddr() *peerAddr {
	id := self.hive.id
	host, port, _ := net.SplitHostPort(self.hive.listenAddr())
	intport, _ := strconv.Atoi(port)
	addr := &peerAddr{
		Addr: self.hive.addr,
		ID:   id[:],
		IP:   net.ParseIP(host),
		Port: uint16(intport),
	}
	return addr
}

// outgoing messages
// send retrieveRequestMsg
func (self *bzz) retrieve(req *retrieveRequestMsgData) error {
	return self.send(retrieveRequestMsg, req)
}

// send storeRequestMsg
func (self *bzz) store(req *storeRequestMsgData) error {
	return self.send(storeRequestMsg, req)
}

// send streamRequestMsg
func (self *bzz) stream(req *streamRequestMsgData) error {
	return self.send(streamRequestMsg, req)
}

// send streamUnsubscribeMsg
func (self *bzz) streamUnsubscribe(req *streamUnsubscribeMsgData) error {
	return self.send(streamUnsubscribeMsg, req)
}

// send transcodeRequestMsg
func (self *bzz) transcode(req *transcodeRequestMsgData) error {
	return self.send(transcodeRequestMsg, req)
}

// send transcodeAckMsg
func (self *bzz) transcodeAck(req *transcodeAckMsgData) error {
	return self.send(transcodeAckMsg, req)
}

func (self *bzz) syncRequest() error {
	req := &syncRequestMsgData{}
	if self.hive.syncEnabled {
		glog.V(logger.Debug).Infof("syncronisation request to peer %v at state %v", self, self.syncState)
		req.SyncState = self.syncState
	}
	if self.syncState == nil {
		glog.V(logger.Warn).Infof("syncronisation disabled for peer %v at state %v", self, self.syncState)
	}
	return self.send(syncRequestMsg, req)
}

// queue storeRequestMsg in request db
func (self *bzz) deliveryRequest(reqs []*syncRequest) error {
	req := &deliveryRequestMsgData{
		Deliver: reqs,
	}
	return self.send(deliveryRequestMsg, req)
}

// batch of syncRequests to send off
func (self *bzz) unsyncedKeys(reqs []*syncRequest, state *syncState) error {
	req := &unsyncedKeysMsgData{
		Unsynced: reqs,
		State:    state,
	}
	return self.send(unsyncedKeysMsg, req)
}

// send paymentMsg
func (self *bzz) Pay(units int, promise swap.Promise) {
	req := &paymentMsgData{uint(units), promise.(*chequebook.Cheque)}
	self.payment(req)
}

// send paymentMsg
func (self *bzz) payment(req *paymentMsgData) error {
	return self.send(paymentMsg, req)
}

// sends peersMsg
func (self *bzz) peers(req *peersMsgData) error {
	return self.send(peersMsg, req)
}

func (self *bzz) protoError(code int, format string, params ...interface{}) (err *errs.Error) {
	err = self.errors.New(code, format, params...)
	err.Log(glog.V(logger.Info))
	return
}

func (self *bzz) send(msg uint64, data interface{}) error {
	if self.hive.blockWrite {
		return fmt.Errorf("network write blocked")
	}
	glog.V(logger.Detail).Infof("-> %v: %v (%T) to %v", msg, data, data, self)
	err := p2p.Send(self.rw, msg, data)
	if err != nil {
		fmt.Println("Error sending in protocol: ", err)
		self.Drop()
	}
	return err
}
