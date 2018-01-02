package com.tigerhuang.gambezi;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.nio.charset.StandardCharsets;

////////////////////////////////////////////////////////////////////////////////
/**
 * Represents a node in the Gambezi Tree
 */
////////////////////////////////////////////////////////////////////////////////
public class _Node {
    // Callbacks
    public OnReadyListener on_ready = null;
    public OnUpdateListener on_update = null;

    // Variables
    public _Node                     __parent       = null;
    public Gambezi                   __gambezi      = null;
    public ArrayList<_Node>          __children     = null;
    public ArrayDeque<SendQueueItem> __send_queue   = null;
    public int                       __refresh_skip = 0;
    public byte[]                    __data         = null;
    public byte[]                    __binary_key   = null;
    public String[]                  __string_key   = null;
    public boolean                   __ready        = false;

	//------------------------------------------------------------------------------
	/**
	 * Constructs a node with a given name, parent key, and gambezi
	 * If the parent key is null, the Node is constructed as the root node
	 */
	public _Node(String name, _Node parent_node, Gambezi parent_gambezi) {
		// Flags
		this.__ready = false;

		this.__parent = parent_node;
		this.__gambezi = parent_gambezi;

		this.__children = new ArrayList<>();
		this.__send_queue = new ArrayDeque<>();

		this.__refresh_skip = parent_gambezi.get_default_subscription();
		this.__data = new byte[0];

		// Init key
		this.__binary_key = new byte[0];
		this.__string_key = new String[0];
		if(parent_node != null) {
			this.__binary_key = Arrays.copyOf(parent_node.__binary_key, parent_node.__binary_key.length+1);
			this.__binary_key[this.__binary_key.length-1] = -1;
			this.__string_key = Arrays.copyOf(parent_node.__string_key, parent_node.__string_key.length+1);
			this.__string_key[this.__string_key.length-1] = name;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the binary key of this node
	 */
	public void _set_binary_key(byte[] key) {
		// Notify ready
		this.__binary_key = key;
		this._set_ready(true);

		// Handle queued actions
		while(this.__send_queue.size() > 0) {
			(this.__send_queue.remove()).run();
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the binary key of this node
	 */
	public byte[] get_binary_key() {
		return this.__binary_key;
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the ID of this node
	 * (-1) indicates no ID assigned yet
	 */
	public int get_id() {
		if(this.__parent != null) {
			return this.__binary_key[this.__binary_key.length - 1];
		}
		else {
			return 0;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the string key of this node
	 */
	public String[] get_string_key() {
		return this.__string_key;
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the name of this node
	 */
	public String get_name() {
		if(this.__parent != null) {
			return this.__string_key[this.__string_key.length - 1];
		}
		else {
			return "";
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the parent of this node
	 */
	public _Node get_parent() {
		return this.__parent;
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the ready state of this node
	 */
	public void _set_ready(boolean ready) {
		// Save state
		this.__ready = ready;

		// Notify ready
		if(ready) {
			// Set refresh skip
			this.set_subscription(this.__refresh_skip);

			if(this.on_ready != null) {
				this.on_ready.on_ready(this);
			}
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Returns if this node is ready to communicate
	 */
	public boolean get_ready() {
		return this.__ready;
	}

	//------------------------------------------------------------------------------
	/**
	 * Updates the subscription for this node
	 *
	 * set_children determines if all descendent keys will be retrieved
	 *
	 * Values for refresh_skip
	 * 0x0000 - get node value updates as soon as they arrive
	 * 0xFFFF - unsubscribe from this key
	 * Any other value of refresh skip indicates that this node will be retrieved
	 * every n client updates
	 */
	public int set_subscription(int refresh_skip) { return this.set_subscription(refresh_skip, false); }
	public int set_subscription(int refresh_skip, boolean set_children) {
		// Save for later usage
		this.__refresh_skip = refresh_skip;

		if(this.__ready) {
			this.__gambezi._set_subscription(this.__binary_key, refresh_skip, set_children);
			return 0;
		}
		else {
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the current subscription of this node
	 */
	public int get_subscription() {
		return this.__refresh_skip;
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
	 * The node being retrieved will be referenced from the current node
	 */
	public _Node get_node(Object string_key) { return this.get_node(string_key, "/"); }
	public _Node get_node(Object string_key, String delimiter) {
		return this.__gambezi.get_node(string_key, delimiter, this);
	}

	//------------------------------------------------------------------------------
	/**
	 * Retrieves all immediate children of this node from the server
	 */
	public int request_children() {
		if(this.__ready) {
			this.__gambezi._request_id(this.__binary_key , "", true, false);
			return 0;
		}
		else {
			final _Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.request_children();
				}
			});
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Retrieves all children of this node recursively from the server
	 */
	public int request_all_children() {
		if(this.__ready) {
			this.__gambezi._request_id(this.__binary_key , "", true, true);
			return 0;
		}
		else {
			final _Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.request_all_children();
				}
			});
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets all children currently visible to this node
	 */
	public Iterable<_Node> get_children() {
		return this.__children;
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the child node with the specified ID
	 * Returns null if the id is not found
	 */
	public _Node _get_child_with_id(int ident) {
		// See if child already exists
		for(_Node child : this.__children) {
			if(child.get_id() == ident) {
				return child;
			}
		}

		// None found
		return null;
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the child node with the specified name
	 * Creates a new child with the name if there is no existing child
	 */
	public _Node _get_child_with_name(String name) {
		// See if child already exists
		for(_Node child : this.__children) {
			if(child.get_name().equals(name)) {
				return child;
			}
		}

		// Create child if nonexistent
		_Node child = new _Node(name, this, this.__gambezi);
		this.__children.add(child);
		return child;
	}

	//==============================================================================
	// Data handling methods
	//==============================================================================

	//------------------------------------------------------------------------------
	/**
	 * Requests the value of a node
	 *
	 * get_children determines if all descendent keys will be retrieved
	 */
	public int request_data() { return this.request_data(false); }
	public int request_data(boolean get_children) {
		if(this.__ready) {
			this.__gambezi._request_data(this.__binary_key, get_children);
			return 0;
		}
		else {
			final _Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.request_data(get_children);
				}
			});
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Data of this node received from server
	 */
	public void _data_received(byte[] data) {
		this.__data = data;

		// Callback if present
		if(this.on_update != null) {
			this.on_update.on_update(this);
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of a node with a byte buffer
	 */
	public int set_data(byte[] data, int offset, int length) {
		if(this.__ready) {
			this.__gambezi._set_data(this.__binary_key, data, offset, length);
			return 0;
		}
		else {
			final _Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.set_data(data, offset, length);
				}
			});
			return 1;
		}
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the data of this node
	 */
	public byte[] get_data() {
		return this.__data;
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of the node as a 64 bit float
	 */
	public int set_double(double value) {
		int length = 8;
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.putDouble(0, value);
		byte[] buf = new byte[length];
		buffer.get(buf, 0, length);
		return this.set_data(buf, 0, length);
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the value of this node as a 64 bit float
	 * Returns NaN as the default if the format does not match
	 */
	public double get_double() {
		int length = 8;
		// Bail if the size is incorrect
		if(this.__data.length != length) {
			return Float.NaN;
		}
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.put(this.__data, 0, length);
		return buffer.getFloat(0);
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of the node as a 32 bit float
	 */
	public int set_float(float value) {
		int length = 4;
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.putFloat(0, value);
		byte[] buf = new byte[length];
		buffer.get(buf, 0, length);
		return this.set_data(buf, 0, length);
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the value of this node as a 32 bit float
	 * Returns NaN as the default if the format does not match
	 */
	public float get_float() {
		int length = 4;
		// Bail if the size is incorrect
		if(this.__data.length != length) {
			return Float.NaN;
		}
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.order(ByteOrder.BIG_ENDIAN);
		buffer.put(this.__data, 0, length);
		return buffer.getFloat(0);
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of the node as a boolean
	 */
	public int set_boolean(boolean value) {
		int length = 1;
		byte[] buf = new byte[length];
		buf[0] = (byte)(value ? 0x01 : 0x00);
		return this.set_data(buf, 0, length);
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the value of this node as a boolean
	 * Returns false as the default if the format does not match
	 */
	public boolean get_boolean() {
		int length = 1;
		// Bail if the size is incorrect
		if(this.__data.length != length) {
			return false;
		}
		return this.__data[0] != 0x00;
	}

	//------------------------------------------------------------------------------
	/**
	 * Sets the value of the node as a string
	 */
	public int set_string(String value) {
		byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
		return this.set_data(buffer, 0, buffer.length);
	}

	//------------------------------------------------------------------------------
	/**
	 * Gets the value of this node as a string
	 */
	public String get_string() {
		return new String(this.__data, 0, this.__data.length, StandardCharsets.UTF_8);
	}
}

interface SendQueueItem {
	public void run();
}
