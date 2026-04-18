package main

import "net"

var pubsubHandler = map[string]func(*ConnState, net.Conn, []Value) Value{
	"SUBSCRIBE":   subscribe,
	"PUBLISH":     publish,
	"UNSUBSCRIBE": unsubscribe,
	"PING":        pingPubSub,
}

func subscribe(cs *ConnState, conn net.Conn, args []Value) Value {
	if len(args) < 1 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'SUBSCRIBE' command"}
	}

	var results []Value

	for _, arg := range args {
		key := arg.Bulk

		alreadySubscribed := false
		for _, subKey := range cs.subState.subscribedKeys {
			if subKey == key {
				alreadySubscribed = true
				break
			}
		}

		if !alreadySubscribed {
			_, err := db.SUBSCRIBE([]string{key}, cs.subState.msgChan)
			if err != nil {
				return Value{Type: ERROR, Str: err.Error()}
			}
			cs.subState.isSubscribed = true
			cs.subState.subscribedKeys = append(cs.subState.subscribedKeys, key)
		}

		results = append(results, Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: "subscribe"},
				{Type: BULK, Bulk: key},
				{Type: INTEGER, Num: len(cs.subState.subscribedKeys)},
			},
		})
	}

	return Value{Type: STREAMS, Array: results}
}

func publish(cs *ConnState, conn net.Conn, args []Value) Value {
	if len(args) < 2 {
		return Value{Type: ERROR, Str: "ERR wrong number of arguments for 'PUBLISH' command"}
	}

	channel := args[0].Bulk
	message := args[1].Bulk

	msgValue := Value{
		Type: ARRAY,
		Array: []Value{
			{Type: BULK, Bulk: "message"},
			{Type: BULK, Bulk: channel},
			{Type: BULK, Bulk: message},
		},
	}

	receivers := db.PUBLISH(channel, msgValue)

	return Value{Type: INTEGER, Num: receivers}
}

func pingPubSub(cs *ConnState, conn net.Conn, args []Value) Value {
	if cs.subState.isSubscribed {
		msg := ""
		if len(args) > 0 {
			msg = args[0].Bulk
		}
		return Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: "pong"},
				{Type: BULK, Bulk: msg},
			},
		}
	}
		
	if len(args) == 0 {
		return Value{Type: STRING, Str: "PONG"}
	}
	return Value{Type: STRING, Str: args[0].Bulk}
}

func unsubscribe(cs *ConnState, conn net.Conn, args []Value) Value {
	var channelsToUnsub []string

	// If no arguments provided, unsubscribe from ALL channels
	if len(args) == 0 {
		channelsToUnsub = append(channelsToUnsub, cs.subState.subscribedKeys...)
	} else {
		for _, arg := range args {
			channelsToUnsub = append(channelsToUnsub, arg.Bulk)
		}
	}

	var results []Value

	for _, channel := range channelsToUnsub {
		// Filter out the channel from the connection's subscribed keys
		newSubscribed := make([]string, 0)
		removed := false
		for _, key := range cs.subState.subscribedKeys {
			if key == channel {
				removed = true
			} else {
				newSubscribed = append(newSubscribed, key)
			}
		}

		if removed {
			cs.subState.subscribedKeys = newSubscribed
			db.UNSUBSCRIBE([]string{channel}, cs.subState.msgChan)
		}

		// If no keys left, exit subscribed mode
		if len(cs.subState.subscribedKeys) == 0 {
			cs.subState.isSubscribed = false
		}

		// Redis returns an array for each channel unsubscribed
		results = append(results, Value{
			Type: ARRAY,
			Array: []Value{
				{Type: BULK, Bulk: "unsubscribe"},
				{Type: BULK, Bulk: channel},
				{Type: INTEGER, Num: len(cs.subState.subscribedKeys)},
			},
		})
	}

	return Value{Type: ARRAY, Array: results}
}
