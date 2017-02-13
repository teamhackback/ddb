/**
Async notification subscription.
This module is only available when Vibe.d is available
*/
module ddb.pg.subscriber;

version(Have_vibe_core):

import ddb.pg.connection : PGConnection;
import ddb.pg.client : PGClient;

import vibe.core.connectionpool : LockedConnection;
import vibe.core.core;
import vibe.core.concurrency;
import vibe.core.log;
import vibe.core.net : TCPConnection;
import vibe.core.sync;
import vibe.core.task;
import vibe.internal.freelistref : FreeListRef;

import core.time : CoreDuration = Duration;

import std.algorithm.searching : all, canFind;
import std.array : array;
import std.concurrency : receiveTimeout;
import std.conv : to;
import std.datetime : days, Duration, msecs, seconds;
import std.exception : enforce;
import std.experimental.allocator : dispose, makeArray, theAllocator;
import std.format : format;
import std.range : takeOne;

alias PGSubscriber = FreeListRef!PGSubscriberImpl;

/**
- listen/unlisten interface: ranges?, single string?
*/

@safe:

/**
Generic sub/pub with over a connection
*/
abstract class SubscriberImpl {
	private {
		bool[string] m_subscriptions;
		string[] m_pendingSubscriptions;
		bool m_listening;
		bool m_stop;
		Task m_listener;
		Task m_listenerHelper;
		Task m_waiter;
		InterruptibleRecursiveTaskMutex m_mutex;
		InterruptibleTaskMutex m_connMutex;
	}

    private enum Action {
		DATA,
		STOP,
		STARTED,
		SUBSCRIBE,
		UNSUBSCRIBE
	}

	@property bool isListening() const {
		return m_listening;
	}

	/// Get a list of channels with active subscriptions
	@property string[] subscriptions() const {
		return () @trusted { return m_subscriptions.keys; } ();
	}

	bool hasSubscription(string channel) const {
		return (channel in m_subscriptions) !is null && m_subscriptions[channel];
	}

	private bool hasNewSubscriptionIn(scope string[] args) {
		bool has_new;
		foreach (arg; args)
			if (!hasSubscription(arg))
				has_new = true;

		return has_new;
	}

	private bool anySubscribed(scope string[] args) {
		bool any_subscribed;
		foreach (arg; args)
			if (hasSubscription(arg))
				any_subscribed = true;

		return any_subscribed;
	}

	this() {
		logTrace("%s: this()", name);
		m_mutex = new InterruptibleRecursiveTaskMutex;
		m_connMutex = new InterruptibleTaskMutex;
	}

	~this() {
		logTrace("%s: ~this", name);
		// TODO: needs to be called in sub-class
        //bstop();
	}

	/// Stop listening and yield until the operation is complete.
	void bstop() {
		logTrace("%s: bstop", name);
		if (!m_listening) return;

		void impl() @safe {
			m_mutex.performLocked!({
				m_waiter = Task.getThis();
				scope(exit) m_waiter = Task();
				stop();

				bool stopped;
				do {
					if (!() @trusted { return receiveTimeout(3.seconds, (Action act) { if (act == Action.STOP) stopped = true;  }); } ())
						break;
				} while (!stopped);

				enforce(stopped, "Failed to wait for Listener to stop");
			});
		}
		inTask(&impl);
	}

	/// Stop listening asynchronously
	void stop() {
		logTrace("%s: stop", name);
		if (!m_listening)
			return;

		void impl() @safe {
			m_mutex.performLocked!({
				m_stop = true;
				() @trusted { m_listener.send(Action.STOP); } ();
				// send a message to wake up the listenerHelper from the reply
				if (m_subscriptions.length > 0) {
					m_connMutex.performLocked!({
                        _unlisten(subscriptions.takeOne.array);
					});
					sleep(30.msecs);
				}
			});
		}
		inTask(&impl);
	}

	/**
    Completes the subscription for a listener to start receiving pubsub messages
	on the corresponding channel(s). Returns instantly if already subscribed.
	If a connection error is thrown here, it stops the listener.
    */
	void subscribe(scope string[] args...)
	{
		logDebug("%s: subscribe: %s", name, args);
		if (!m_listening) {
			foreach (arg; args)
				m_pendingSubscriptions ~= arg;
			return;
		}

		if (!hasNewSubscriptionIn(args))
			return;

		void impl() @safe {
			try {
				m_mutex.performLocked!({
					m_waiter = Task.getThis();
					scope(exit) m_waiter = Task();
					bool subscribed;
					m_connMutex.performLocked!({
						_listen(args);
					});
					/* TODO: this mechanism isn't used atm (Postgres doesn't return acks)
					// TODO: do we search for the first occurrence or for all?
					while(!args.all!(arg => hasSubscription(arg))) {
						if (!() @trusted { return receiveTimeout(2.seconds, (Action act) { enforce(act == Action.SUBSCRIBE);  }); } ())
							break;

						subscribed = true;
					}
					debug {
						auto keys = () @trusted { return m_subscriptions.keys; } ();
						logTrace("%s: Can find keys?: %s",  name, keys.canFind(args));
						logTrace("%s: Subscriptions: %s", name, keys);
					}
					enforce(subscribed, "Could not complete subscription(s).");
					*/
				});
			} catch (Exception e) {
				logDebug("%s subscribe() failed: %s", name, e.msg);
				bstop();
			}
		}
		inTask(&impl);
	}

	/**
    Unsubscribes from the channel(s) specified, returns immediately if none
	is currently being listened.
	If a connection error is thrown here, it stops the listener.
    */
	void unsubscribe(scope string[] args...)
	{
		logTrace("%s: unsubscribe", name);

		void impl() @safe {

			if (!anySubscribed(args))
				return;

			scope(failure) bstop();
			assert(m_listening);

			m_mutex.performLocked!({
				m_waiter = Task.getThis();
				scope(exit) m_waiter = Task();
				bool unsubscribed;
				m_connMutex.performLocked!({
					_unlisten(args);
				});
				/*
				while(() @trusted { return m_subscriptions.byKey.canFind(args); } ()) {
					if (!() @trusted { return receiveTimeout(2.seconds, (Action act) { enforce(act == Action.UNSUBSCRIBE);  }); } ()) {
						unsubscribed = false;
						break;
					}
					unsubscribed = true;
				}
				debug {
					auto keys = () @trusted { return m_subscriptions.keys; } ();
					logTrace("Can find keys?: %s",  keys.canFind(args));
					logTrace("Subscriptions: %s", keys);
				}
				enforce(unsubscribed, "Could not complete unsubscription(s).");
                */
			});
		}
		inTask(&impl);
	}

	private void inTask(scope void delegate() @safe impl) {
		logTrace("%s: inTask", name);
		if (Task.getThis() == Task())
		{
			Throwable ex;
			bool done;
			Task task = runTask({
				logDebug("%s: inTask %s", name, Task.getThis());
				try impl();
				catch (Exception e) {
					ex = e;
				}
				done = true;
			});
			task.join();
			logDebug("done");
			if (ex)
				throw ex;
		}
		else
			impl();
	}

	// Same as listen, but blocking
	void blisten(void delegate(string, string) @safe onMessage, Duration timeout = 0.seconds)
	{
		init();

        // TODO: this mechanism isn't used atm
		void onSubscribe(string channel) @safe {
			logTrace("%s: Callback subscribe(%s)", name, channel);
			m_subscriptions[channel] = true;
			if (m_waiter != Task())
				() @trusted { m_waiter.send(Action.SUBSCRIBE); } ();
		}

		void onUnsubscribe(string channel) @safe {
			logTrace("%s: Callback unsubscribe(%s)", name, channel);
			m_subscriptions.remove(channel);
			if (m_waiter != Task())
				() @trusted { m_waiter.send(Action.UNSUBSCRIBE); } ();
		}

		void teardown() @safe { // teardown
			logDebug("%s exiting", name);
			// More publish commands may be sent to this connection after recycling it, so we
			// actively destroy it
			Action act;
			// wait for the listener helper to send its stop message
			while (act != Action.STOP)
				act = () @trusted { return receiveOnly!Action(); } ();
			close();
			m_listening = false;
			return;
		}

		// Waits for data and advises the handler
		m_listenerHelper = runTask({
			while(true) {
				if (!m_stop && waitForData(100.msecs)) {
					// We check every 100 ms if this task should stay active
					if (m_stop)	break;
					else if (!dataAvailableForRead) continue;
					// Data has arrived, this task is in charge of notifying the main handler loop
					logDebug("%s: Notify data arrival", name);

					() @trusted { receiveTimeout(0.seconds, (Variant v) {}); } (); // clear message queue
					() @trusted { m_listener.send(Action.DATA); } ();
					if (!() @trusted { return receiveTimeout(5.seconds, (Action act) { assert(act == Action.DATA); }); } ())
						assert(false);

				} else if (m_stop || !connected) break;
				logDebug("%s: No data arrival in 100 ms...", name);
			}
			logDebug("%s: Helper exit.", name);
			() @trusted { m_listener.send(Action.STOP); } ();
		} );

		m_listening = true;
		logDebug("%s now listening", name);
		if (m_waiter != Task())
			() @trusted { m_waiter.send(Action.STARTED); } ();

		if (timeout == 0.seconds)
			timeout = 1000.days; // make sure 0.seconds is considered as big.

		scope(exit) {
			logDebug("%s, exit.", name);
			if (!m_stop) {
				stop(); // notifies the listenerHelper
			}
			m_listenerHelper.join();
			// close the data connections
			teardown();

			if (m_waiter != Task())
				() @trusted { m_waiter.send(Action.STOP); } ();

			m_listenerHelper = Task();
			m_listener = Task();
			m_stop = false;
		}

		// Start waiting for data notifications to arrive
		while(true) {
			auto handler = (Action act) {
				if (act == Action.STOP) m_stop = true;
				if (m_stop) return;
				logDebug("%s: Calling PubSub Handler", name);
				m_connMutex.performLocked!({
                    pubsubHandler(onMessage, &onSubscribe, &onUnsubscribe, &teardown); // handles one command at a time
				});
				() @trusted { m_listenerHelper.send(Action.DATA); } ();
			};

			if (!() @trusted { return receiveTimeout(timeout, handler); } () || m_stop) {
				logDebug("%s stopped", name);
				break;
			}
		}
	}

	/**
    Waits for messages and calls the callback with the channel and the message as arguments.
	The timeout is passed over to the listener, which closes after the period of inactivity.
	Use 0.seconds timeout to specify a very long time (10 years)
	Errors will be sent to Callback Delegate on channel "Error".
    */
	Task listen(void delegate(string, string) @safe callback, Duration timeout = 0.seconds)
	{
		logTrace("Listen");
		void impl() @safe {
			logTrace("Listen");
			m_waiter = Task.getThis();
			scope(exit) m_waiter = Task();
			Throwable ex;
			m_listener = runTask({
				try blisten(callback, timeout);
				catch(Exception e) {
					ex = e;
					if (m_waiter != Task() && !m_listening) {
						() @trusted { m_waiter.send(Action.STARTED); } ();
						return;
					}
					callback("Error", e.msg);
				}
			});
			m_mutex.performLocked!({
				() @trusted { receiveTimeout(2.seconds, (Action act) { assert( act == Action.STARTED); }); } ();
				if (ex) throw ex;
				enforce(m_listening, "Failed to start listening, timeout of 2 seconds expired");
			});

			foreach (channel; m_pendingSubscriptions) {
				subscribe(channel);
			}

			m_pendingSubscriptions = null;
		}
		inTask(&impl);
		return m_listener;
	}

	abstract string name();

    abstract protected void init();
    abstract protected void close();
	abstract void _listen(scope string[] channels);
	abstract void _unlisten(scope string[] channels);

	abstract protected void pubsubHandler(
        scope void delegate(string channel, string message) @safe onMessage,
        scope void delegate(string channel) @safe onSubscribe,
        scope void delegate(string channel) @safe onUnsubscribe,
        scope void delegate() @safe teardown
	);

	abstract protected bool waitForData(CoreDuration timeout = CoreDuration.max);
	abstract protected bool dataAvailableForRead();
	abstract protected bool connected();
}

final class PGSubscriberImpl : SubscriberImpl
{
    private {
	    LockedConnection!PGConnection m_lockedConnection;
		PGClient m_client;
		alias conn = m_lockedConnection;
	}

	this(PGClient client) {
        super();
		m_client = client;
	}
	~this()
	{
        logDebug("PGSubscriberImpl: ~this");
        bstop();
	}

	override string name()
    {
        return "PGListener";
    }

	override protected void init()
	{
	    import vibe.core.net : connectTCP;
		logTrace("%s init", name);
		if (!m_lockedConnection) {
			m_lockedConnection = m_client.lockConnection();
		}

        // try to reconnect if connection closed
        // TODO: is this necessary?
		//if (!m_lockedConnection.conn || !m_lockedConnection.conn.connected) {
			//try m_lockedConnection.conn = connectTCP(m_lockedConnection.m_host, m_lockedConnection.m_port);
			//catch (Exception e) {
				//throw new Exception(format("Failed to connect to Server at %s:%s.", m_lockedConnection.m_host, m_lockedConnection.m_port), __FILE__, __LINE__, e);
			//}
			//m_lockedConnection.conn.tcpNoDelay = true;
		//}
	}

	override protected void close()
	{
		logTrace("%s close", name);
		m_lockedConnection.conn.close();
		m_lockedConnection.destroy();
	}

	override protected bool waitForData(CoreDuration timeout)
    {
		return m_lockedConnection.conn && m_lockedConnection.conn.waitForData(timeout);
    }

	override protected bool dataAvailableForRead()
    {
		return m_lockedConnection.conn && m_lockedConnection.conn.dataAvailableForRead;
    }

    override protected bool connected()
    {
        return !!m_lockedConnection.conn;
    }

	override protected void pubsubHandler(
        scope void delegate(string channel, string message) @safe onMessage,
        scope void delegate(string channel) @safe onSubscribe,
        scope void delegate(string channel) @safe onUnsubscribe,
        scope void delegate() @safe teardown
	)
    {
        logDebug("%s: pubsubHandler", name);
        try
        {
            auto msg = nextNotification;
		    onMessage(msg.channel, msg.payload);
		} catch (Exception e)
		{
            logDebug("%s: pubsubhandler exception: %s", name, e.msg);
		}
    }

    /**
    Subscribes to a channel.
    Params:
        channel = name of the channel to subscribe
    */
    void _listen(string channel)
    {
        // TODO: register callback
        // we can't use the `execute` interface here as it tries to read the response
        conn.sendQueryMessage(`LISTEN "` ~ channel ~ `"`);
    }

    override void _listen(scope string[] channels)
    {
        foreach (channel; channels)
            _listen(channel);
    }

    /**
    Unsubscribes from a channel.
    Params:
        channel = name of the channel to unsubscribe
    */
    void _unlisten(string channel)
    {
       conn.sendQueryMessage(`UNLISTEN "` ~ channel ~ `"`);
    }

    override void _unlisten(scope string[] channels)
    {
        foreach (channel; channels)
            _unlisten(channel);
    }

    /**
    Sends a notification to channel with a payload
    */
    void notify(string channel, string payload)
    {
        // TODO: add quote escaping
        // we can't use the `execute` interface here as it tries to read the response
        conn.notify(channel, payload);
    }

    alias publish = notify;

    static struct Notification { string channel, payload; }

    /**
    Blocks until the next notification is received
    */
    Notification nextNotification()
    {
         import ddb.pg.exceptions;
         import ddb.pg.types;

    receive:

        auto msg = conn.getMessage();
        with (PGResponseMessageTypes)
        switch (msg.type)
        {
            case ErrorResponse:
                auto response = conn.handleResponseMessage(msg);
                throw new PGServerErrorException(response);
            case NotificationResponse:
                int msgLength = msg.read!int;
                string channelName = msg.readCString;
                string payload = msg.readCString;
                return Notification(channelName, payload);
            case ReadyForQuery:
            case NoticeResponse:
            case ParameterStatus:
            default:
                goto receive;
        }
    }
}
