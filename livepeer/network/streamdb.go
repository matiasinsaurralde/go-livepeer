package network

import "github.com/livepeer/go-livepeer/livepeer/storage/streaming"

type StreamDB struct {
	DownstreamRequesters        map[streaming.StreamID][]*peer
	UpstreamTranscodeRequesters map[streaming.StreamID]*peer
	TranscodedStreams           map[streaming.StreamID][]transcodedStreamData
	UpstreamProviders           map[streaming.StreamID][]*peer
}

func NewStreamDB() *StreamDB {
	return &StreamDB{
		DownstreamRequesters:        make(map[streaming.StreamID][]*peer),
		UpstreamTranscodeRequesters: make(map[streaming.StreamID]*peer),
		TranscodedStreams:           make(map[streaming.StreamID][]transcodedStreamData),
		UpstreamProviders:           make(map[streaming.StreamID][]*peer),
	}
}

func (self *StreamDB) AddDownstreamPeer(streamID streaming.StreamID, p *peer) {
	self.DownstreamRequesters[streamID] = append(self.DownstreamRequesters[streamID], p)
}

func (self *StreamDB) AddUpstreamTranscodeRequester(transcodeID streaming.StreamID, p *peer) {
	self.UpstreamTranscodeRequesters[transcodeID] = p
}

func (self *StreamDB) AddTranscodedStream(originalStreamID streaming.StreamID, transcodedStream transcodedStreamData) {
	self.TranscodedStreams[originalStreamID] = append(self.TranscodedStreams[originalStreamID], transcodedStream)
}

func (self *StreamDB) AddUpstreamPeer(streamID streaming.StreamID, p *peer) {
	self.UpstreamProviders[streamID] = append(self.UpstreamProviders[streamID], p)
}

func (self *StreamDB) ContainsUpstreamPeer(streamID streaming.StreamID, p *peer) bool {
	for _, val := range self.UpstreamProviders[streamID] {
		if p == val {
			return true
		}
	}
	return false
}

func (self *StreamDB) DeleteDownstreamPeer(streamID streaming.StreamID, p *peer) {
	reqs := self.DownstreamRequesters[streamID]
	length := len(reqs)

	for i, val := range reqs {
		if val == p {
			reqs[i] = reqs[length-1]
			reqs = reqs[:length-1]
			break
		}
	}
}
