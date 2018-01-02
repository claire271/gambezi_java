package com.tigerhuang.gambezi;
 
import java.util.Arrays;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ServerHandshake;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

////////////////////////////////////////////////////////////////////////////////
/**
 * Represents a connection to a gambezi server
 */
////////////////////////////////////////////////////////////////////////////////
public class Gambezi implements GambeziWebsocketListener {
	// Callbacks
	public OnReadyListener on_ready = null;
	public OnErrorListener on_error = null;
	public OnCloseListener on_close = null;

	// Variables
	public ArrayDeque<String[]>   __key_request_queue    = null;
	public _Node                  __root_node            = null;
	public int                    __refresh_rate         = 0;
	public String                 __host_address         = null;
	public boolean                __ready                = false;
	public GambeziWebsocketClient __websocket            = null;
	public int                    __default_subscription = 0;
	public boolean                __reconnect            = false;
	public double                 __reconnect_interval   = 0;
	public boolean                __heartbeat            = false;

	//------------------------------------------------------------------------------
	/**
	 * Constructs a gambezi instance with the given target host
	 */
	public Gambezi(String host_address) { this(host_address, true); }
	public Gambezi(String host_address, boolean reconnect) { this(host_address, reconnect, 5); }
	public Gambezi(String host_address, boolean reconnect, double reconnect_interval) {

		// Init
		this.__root_node = new _Node(null, null, this);
		this.__refresh_rate = 100;
		this.__host_address = host_address;
		this.__default_subscription = 1;
		this.__reconnect = reconnect;
		this.__reconnect_interval = reconnect_interval;
		this.__heartbeat = true;

		// Attempt to open connection
		this.open_connection();

		// Setup heartbeat
		this.__root_node.set_subscription((int)Math.round(this.__reconnect_interval * 1000 / this.__refresh_rate / 2));
		final Gambezi gambezi = this;
		this.__root_node.on_update = new OnUpdateListener() {
			public void on_update(Object node) {
				gambezi.__heartbeat = true;
			}
		};

		// Heartbeat monitoring
		final boolean reconnect_parameter = reconnect;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				//Heartbeat not found
				if(!gambezi.__heartbeat) {
					gambezi.close_connection();

					// Reopen if requested to
					if(reconnect_parameter) {
						gambezi.open_connection();
					}
				}

				// Clear heartbeat
				gambezi.__heartbeat = false;
			}
		}, Math.round(reconnect_interval * 1000), Math.round(reconnect_interval * 1000));
	}

	//==============================================================================
	// Gambezi client methods
	//==============================================================================

	//------------------------------------------------------------------------------
	/**
	 * Connects this gambezi instance to the server
	 */
	public int open_connection() {
		// Bail if the connection is still open
		if(this.__ready) {
			return 1;
		}

		// Clear queue
		this.__key_request_queue = new ArrayDeque<>();

		// Set flags
		this.__ready = false;

		// Mark all nodes as not ready to communicate
		this.__unready_nodes(this.__root_node);

		// Websocket init
		HashMap<String, String> headers = new HashMap<>();
		headers.put("Sec-WebSocket-Protocol", "gambezi-protocol");
		URI uri = null;
		try {
			uri = new URI("ws://" + this.__host_address);
		}
		catch(URISyntaxException ex) {
			if(this.on_error != null) {
				this.on_error.on_error(ex);
			}
		}
		this.__websocket = new GambeziWebsocketClient(uri, headers, this);
		this.__websocket.connect();

		// Success
		return 0;
	}

	//------------------------------------------------------------------------------
	/**
	 * Closes this gambezi connection
	 */
	public void close_connection() {
		if(this.__websocket != null) {
			this.__websocket.close();
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Returns whether this gambezi instance is ready to communicate
	 */
	public boolean is_ready() {
		return this.__ready;
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the refresh rate of this client in milliseconds
	 */
	public int set_refresh_rate(int refresh_rate) {
		// Save for later usage
		this.__refresh_rate = refresh_rate;

		// Update heartbeat
		this.__root_node.set_subscription((int)Math.round(this.__reconnect_interval * 1000 / this.__refresh_rate / 2));

		if(this.__ready) {
			// Create buffer
			byte[] buf = new byte[3];

			// Header
			buf[0] = 0x02;

			// Length
			buf[1] = (byte)((refresh_rate >> 8) & 0xFF);
			buf[2] = (byte)((refresh_rate) & 0xFF);

			// Send packet
			this.__websocket.send(buf);
			return 0;
		}
		else {
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the refresh rate of this client in milliseconds
	 */
	public int get_refresh_rate() {
		return this.__refresh_rate;
	}

	//------------------------------------------------------------------------------
	/*
	 * Sets the default subscription rate for this client. Changes are not applied
	 * retroactively
	 */
	public void set_default_subscription(int default_subscription) {
		this.__default_subscription = default_subscription;
	}

	//------------------------------------------------------------------------------
	/*
	 * Gets the default subscription rate for this client
	 */
	public int get_default_subscription() {
		return this.__default_subscription;
	}

	//==============================================================================
	// Tree information methods
	//==============================================================================

	//------------------------------------------------------------------------------
	/**
	 * Gets a node with the given name as a child of the given node.
	 *
	 * If string_key is a string array, each element of the array is considered a
	 * level in the tree. If string_key is a single string, the string is split by
	 * the delimiter and each resulting element is considered a level in the tree.
	 *
	 * If parent_node is not given, the key is referenced from the root node.
	 */
	public _Node get_node(Object string_key) { return this.get_node(string_key, "/"); }
	public _Node get_node(Object string_key, String delimiter) { return this.get_node(string_key, delimiter, null); }
	public _Node get_node(Object string_key, String delimiter, _Node parent_node) {
		// Handle the case of the root node
		if(parent_node == null) {
			parent_node = this.__root_node;
		}

		// Split string_key if necessary
		if(!string_key.getClass().isArray()) {
			string_key = ((String)string_key).split(delimiter);
		}

		// Build new string key
		String[] parent_key = parent_node.get_string_key();
		String[] child_key = (String[])string_key;
		String[] final_key = new String[parent_key.length + child_key.length];
		System.arraycopy(parent_key, 0, final_key, 0, parent_key.length);
		System.arraycopy(string_key, 0, final_key, parent_key.length, child_key.length);

		// Request node
		return this.__request_node(final_key);
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the root node
	 */
	public _Node get_root_node() {
		return this.__root_node;
	}

	//------------------------------------------------------------------------------
	/**
	 * Registers a string key and gets the corresponding node
	 */
	public _Node __request_node(String[] string_key) {
		// Queue up the ID requests and get the node
		_Node node = this.__root_node;
		for(int i = 0;i < string_key.length;i++) {
			// Go down one level
			node = node._get_child_with_name(string_key[i]);

			// Queue up ID request if needed and already connected
			if(this.__ready) {
				if(node.get_id() < 0) {
					this.__key_request_queue.push(Arrays.<String>copyOfRange(string_key, 0, i+1));
				}
			}
		}

		// Get any IDs necessary if already connected
		if(this.__ready) {
			this.__process_key_request_queue();
		}

		// Return
		return node;
	}

	//------------------------------------------------------------------------------
	/**
	 * Requests the ID of a node for a given parent key and name
	 *
	 * get_children determines if all descendent keys will be retrieved
	 *
	 * get_children_all determines if all descendent keys will be retrieved
	 * recursively
	 */
	public void _request_id(byte[] parent_key, String name, boolean get_children, boolean get_children_all) {
		// This method is always guarded when called, so no need to check readiness
		byte[] name_bytes = name.getBytes(StandardCharsets.UTF_8);

		// Create buffer
		byte[] buf = new byte[parent_key.length + name_bytes.length + 4];

		// Header
		buf[0] = 0x00;
		buf[1] = (byte)((get_children_all ? 2 : 0) | (get_children ? 1 : 0));

		// Parent key
		buf[2] = (byte)(parent_key.length);
		for(int i = 0;i < parent_key.length;i++) {
			buf[i + 3] = parent_key[i];
		}

		// Name
		buf[3 + parent_key.length] = (byte)(name_bytes.length);
		for(int i = 0;i < name_bytes.length;i++) {
			buf[i + 4 + parent_key.length] = name_bytes[i];
		}

		// Send data
		this.__websocket.send(buf);
	}

	//------------------------------------------------------------------------------
	/**
	 * Processes string key requests in the queue until one succeeds
	 */
	public void __process_key_request_queue() {
		// This method is always guarded when called, so no need to check readiness

		// Process entires until one succeeds without an error
		while(this.__key_request_queue.size() > 0) {
			int code = 0;

			// Build the binary parent key
			String[] string_key = this.__key_request_queue.remove();
			byte[] parent_binary_key = new byte[string_key.length - 1];
			_Node node = this.__root_node;
			for(int i = 0;i < string_key.length - 1;i++) {
				node = node._get_child_with_name(string_key[i]);
				int ident = node.get_id();
				// Bail if the parent does not have an ID
				if(ident < 0) {
					code = 1;
					break;
				}
				parent_binary_key[i] = (byte)(ident);
			}

			// Error when building binary key
			if(code > 0) {
				if(this.on_error != null) {
					this.on_error.on_error("Error processing ID queue");
				}
			}
			// No error
			else {
				// Request the ID
				String name = string_key[string_key.length - 1];
				this._request_id(parent_binary_key, name, false, false);
				break;
			}
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Recursive method to fetch all IDs for all nodes
	 */
	public void __queue_id_requests(_Node node, String[] parent_string_key) {
        // Normal node
		String[] string_key = null;
		if(parent_string_key != null) {
			string_key = Arrays.copyOf(parent_string_key, parent_string_key.length+1);
			string_key[parent_string_key.length] = node.get_name();
            this.__key_request_queue.add(string_key);
		}
        // Root node
		else {
			string_key = new String[0];
		}

        // Process children
        for(_Node child : node.get_children()) {
            this.__queue_id_requests(child, string_key);
		}
	}

	//------------------------------------------------------------------------------
    /**
	 * Recursive method to set all child nodes to not ready
	 */
	public void __unready_nodes(_Node node) {
        // Set node state
        node._set_ready(false);

        // Process children
        for(_Node child : node.get_children()) {
            this.__unready_nodes(child);
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the node for a given binary key
	 *
	 * get_parent determines if the immediate parent of the binary
	 * key will be retrieved instead
	 */
	public _Node __traverse_tree(byte[] binary_key, boolean get_parent) {
		_Node node = this.__root_node;
		for(int i = 0;i < binary_key.length - (get_parent ? 1 : 0);i++) {
			node = node._get_child_with_id(binary_key[i]);
			// Bail if the key is bad
			if(node == null) {
				return null;
			}
		}
		return node;
	}

	//==============================================================================
	// Individual node methods
	//==============================================================================

	//------------------------------------------------------------------------------
	/**
	 * Requests the value of a node
	 *
	 * get_children determines if all descendent keys will
	 * be retrieved
	 */
	public void _request_data(byte[] key, boolean get_children) {
		// This method is always guarded when called, so no need to check readiness

		// Create buffer
		byte[] buf = new byte[key.length + 3];

		// Header
		buf[0] = 0x04;
		buf[1] = (byte)(get_children ? 1 : 0);

		// Key
		buf[2] = (byte)(key.length);
		for(int i = 0;i < key.length;i++) {
			buf[i + 3] = key[i];
		}

		// Send packet
		this.__websocket.send(buf);
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of a node with a byte buffer
	 */
	public void _set_data(byte[] key, byte[] data, int offset, int length) {
		// This method is always guarded when called, so no need to check readiness

		// Create buffer
		byte[] buf = new byte[key.length + length + 4];

		// Header
		buf[0] = 0x01;

		// Key
		buf[1] = (byte)(key.length);
		for(int i = 0;i < key.length;i++) {
			buf[i + 2] = key[i];
		}

		// Length
		buf[2 + key.length] = (byte)((length >> 8) & 0xFF);
		buf[3 + key.length] = (byte)((length) & 0xFF);

		// Value
		for(int i = 0;i < length;i++) {
			buf[i + 4 + key.length] = data[i + offset];
		}

		// Send packet
		this.__websocket.send(buf);
	}

	//------------------------------------------------------------------------------
	/**
	 * Updates the subscription for a paticular key
	 *
	 * set_children determines if all descendent keys will be retrieved
	 *
	 * Values for refresh_skip
	 * 0x0000 - get node value updates as soon as they arrive
	 * 0xFFFF - unsubscribe from this key
	 * Any other value of refresh skip indicates that this node will be retrieved
	 * every n client updates
	 */
	public void _set_subscription(byte[] key, int refresh_skip, boolean set_children) {
		// This method is always guarded when called, so no need to check readiness

		// Create buffer
		byte[] buf = new byte[key.length + 5];

		// Header
		buf[0] = 0x03;
		buf[1] = (byte)(set_children ? 1 : 0);
		buf[2] = (byte)((refresh_skip >> 8) & 0xFF);
		buf[3] = (byte)((refresh_skip) & 0xFF);

		// Key
		buf[4] = (byte)(key.length);
		for(int i = 0;i < key.length;i++) {
			buf[i + 5] = key[i];
		}

		// Send packet
		this.__websocket.send(buf);
	}

	//==============================================================================
	// Gambezi to websocket callbacks
	//==============================================================================

	//------------------------------------------------------------------------------
	/**
	 * Callback when there is a websocket error
	 */
	public void __on_error(Exception ex) {
		if(this.on_error != null) {
			this.on_error.on_error(ex);
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Callback when websockets gets initialized
	 */
	public void __on_open(ServerHandshake handshakeData) {
		// Set is ready state
		this.__ready = true;

		// Set refresh rate
		this.set_refresh_rate(this.__refresh_rate);

		// Queue all IDs for all nodes
		this.__queue_id_requests(this.__root_node, null);

		// Get the next queued ID request
        this.__process_key_request_queue();

        // Set root node
        this.__root_node._set_ready(true);

        // Notify of ready state
		if(this.on_ready != null) {
			this.on_ready.on_ready(handshakeData);
		}
	}

	/**
	 * Callback when websockets is closed
	 */
	public void __on_close(HashMap<String, Object> closeData) {
        this.__ready = false;

        // Mark all nodes as not ready to communicate
        this.__unready_nodes(this.__root_node);

        // Notify of closed state
        if(this.on_close != null) {
            this.on_close.on_close(closeData);
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Callback when the client recieves a packet from the server
	 */
	public void __on_message(byte[] buf) {

		////////////////////////////////////////
		// ID response from server
		if(buf[0] == 0) {
			// Extract binary key
			byte[] binary_key = new byte[buf[1] & 0xFF];
			for(int i = 0;i < binary_key.length;i++) {
				binary_key[i] = buf[i + 2];
			}

			// Extract name
			int name_length = buf[binary_key.length + 2] & 0xFF;
            int name_offset = binary_key.length + 3;
			String name = new String(buf, name_offset, name_length, StandardCharsets.UTF_8);

			// Bail if the root node got requested
			if(binary_key.length == 0) {
				// Get the next queued ID request
				this.__process_key_request_queue();
				return;
			}

			// Get the matching node and set the ID
			_Node node = this.__traverse_tree(binary_key, true);
			// No error
			if(node != null) {
				node = node._get_child_with_name(name);
				node._set_binary_key(binary_key);

				// Get the next queued ID request
				this.__process_key_request_queue();
			}
		}

		////////////////////////////////////////
		// Value update from server
		else if(buf[0] == 1) {
			// Extract binary key
			byte[] binary_key = new byte[buf[1] & 0xFF];
			for(int i = 0;i < binary_key.length;i++) {
				binary_key[i] = buf[i + 2];
			}

			// Extract data
			int data_length = ((buf[binary_key.length + 2] & 0xFF) << 8) | (buf[binary_key.length + 3] & 0xFF);
			byte[] data = new byte[data_length];
			for(int i = 0;i < data_length;i++) {
				data[i] = buf[binary_key.length + 4 + i];
			}

			// Get the matching node and set the data
			_Node node = this.__traverse_tree(binary_key, false);
			// No error
			if(node != null) {
				node._data_received(data);
			}
		}

		////////////////////////////////////////
		// Error message from server
		else if(buf[0] == 2) {
			// Extract message
			String message = new String(buf, 2, buf[1] & 0xFF, StandardCharsets.UTF_8);
			// Use the message
			if(this.on_error != null) {
				this.on_error.on_error(message);
			}
		}
	}
}


////////////////////////////////////////////////////////////////////////////////
/**
 * Connector to the websocket library
 */
class GambeziWebsocketClient extends WebSocketClient {
	public GambeziWebsocketListener gambeziWebsocketListener;

	// Constructor
	public GambeziWebsocketClient(URI serverUri, HashMap<String, String> headers, GambeziWebsocketListener gambeziWebsocketListener) {
		// Start websocket
		super(serverUri, new Draft_6455(), headers, 10);

		// Save listener
		this.gambeziWebsocketListener = gambeziWebsocketListener;
	}

	// Connection opened
	public void onOpen(ServerHandshake handshakeData) {
		this.gambeziWebsocketListener.__on_open(handshakeData);
	}

	// Ignore string messages
	public void onMessage(String message) {}

	// Process binary messages
	public void onMessage(ByteBuffer blob) {
		blob.rewind();
		byte[] buf = new byte[blob.remaining()];
		blob.get(buf);
		this.gambeziWebsocketListener.__on_message(buf);
	}

	// Connection closed
	public void onClose(int code, String reason, boolean remote) {
		HashMap<String, Object> closeData = new HashMap<>();
		closeData.put("code", code);
		closeData.put("reason", reason);
		closeData.put("remote", remote);
		this.gambeziWebsocketListener.__on_close(closeData);
	}

	// Error occured
	public void onError(Exception ex) {
		this.gambeziWebsocketListener.__on_error(ex);
	}
}


////////////////////////////////////////////////////////////////////////////////
/**
 * Listener for websocket events
 */
interface GambeziWebsocketListener {
	public void __on_error(Exception ex);
	public void __on_open(ServerHandshake handshakeData);
	public void __on_close(HashMap<String, Object> closeData);
	public void __on_message(byte[] buf);
}
