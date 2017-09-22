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
public class Node {
    // Callbacks
    public OnReadyListener  on_ready = null;
    public OnUpdateListener on_update = null;

    // Variables
    public String                    __name = null;
    public Gambezi                   __gambezi = null;
    public ArrayList<Node>           __children = null;
    public ArrayDeque<SendQueueItem> __send_queue = null;
    public int                       __refresh_skip = 0xFFFF;
    public byte[]                    __data = null;
    public byte[]                    __key = null;
    public boolean                   __ready = false;

	/**
	 * Constructs a node with a given name, parent key, and gambezi
	 * If the parent key is null, the Node is constructed as the root node
	 */
	public Node(String name, byte[] parent_key, Gambezi parent_gambezi) {
		// Flags
		this.__ready = false;

		this.__name = name;
		this.__gambezi = parent_gambezi;

		this.__children = new ArrayList<>();
		this.__send_queue = new ArrayDeque<>();

		this.__refresh_skip = 0xFFFF;
		this.__data = new byte[0];

		// Init key
		this.__key = new byte[0];
		if(parent_key != null) {
			this.__key = Arrays.copyOf(parent_key, parent_key.length+1);
			this.__key[parent_key.length] = -1;
		}
	}

	/**
	 * Gets all children currently visible to this node
	 */
	public Iterable<Node> get_children() {
		return this.__children;
	}

	/**
	 * Gets the ID of this node
	 * (-1) indicates no ID assigned yet
	 */
	public int get_id() {
		return this.__key[this.__key.length - 1];
	}

	/**
	 * Gets the name of this node
	 */
	public String get_name() {
		return this.__name;
	}

	/**
	 * Sets the binary key of this node
	 */
	public void _set_key(byte[] key) {
		// Notify ready
		this.__key = key;
		this._set_ready(true);

		// Handle queued actions
		while(this.__send_queue.size() > 0) {
			(this.__send_queue.remove()).run();
		}
	}

	/**
	 * Gets the binary key of this node
	 */
	public byte[] get_key() {
		return this.__key;
	}

	/**
	 * Sets the data of this node
	 */
	public void _set_data(byte[] data) {
		this.__data = data;
	}

	/**
	 * Gets the data of this node
	 */
	public byte[] get_data() {
		return this.__data;
	}

	/**
	 * Sets the ready state of this node
	 */
	public void _set_ready(boolean ready) {
		// Save state
		this.__ready = ready;

		// Notify ready
		if(ready) {
			// Set refresh skip
			this.update_subscription(this.__refresh_skip);

			if(this.on_ready != null) {
				this.on_ready.on_ready();
			}
		}
	}

	/**
	 * Returns if this node is ready to communicate
	 */
	public boolean is_ready() {
		return this.__ready;
	}

	/**
	 * Gets the child node with the specified name
	 * Creates a new child with the name if there is no existing child
	 */
	public Node get_child_with_name(String name) { return this.get_child_with_name(name, false); }
	public Node get_child_with_name(String name, boolean create) {
		// See if child already exists
		for(Node child : this.__children) {
			if(child.get_name().equals(name)) {
				return child;
			}
		}

		// Bail if requested not to create
		if(!create) {
			return null;
		}

		// Create child if nonexistent
		Node child = new Node(name, this.__key, this.__gambezi);
		this.__children.add(child);
		return child;
	}

	/**
	 * Gets the child node with the specified ID
	 * Returns null if the id is not found
	 */
	public Node _get_child_with_id(int ident) {
		// See if child already exists
		for(Node child : this.__children) {
			if(child.get_id() == ident) {
				return child;
			}
		}

		// None found
		return null;
	}

	/**
	 * Sets the value of a node with a byte buffer
	 */
	public int set_data_raw(byte[] data, int offset, int length) {
		if(this.__ready) {
			this.__gambezi._set_data_raw(this.__key, data, offset, length);
			return 0;
		}
		else {
			final Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.set_data_raw(data, offset, length);
				}
			});
			return 1;
		}
	}

	/**
	 * Requests the value of a node
	 *
	 * get_children determines if all descendent keys will
	 * be retrieved
	 */
	public int request_data() { return this.request_data(false); }
	public int request_data(boolean get_children) {
		if(this.__ready) {
			this.__gambezi._request_data(this.__key, get_children);
			return 0;
		}
		else {
			final Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.request_data(get_children);
				}
			});
			return 1;
		}
	}

	/**
	 * Updates the subscription for this node
	 *
	 * set_children determines if all descendent keys will
	 * be retrieved
	 *
	 * Values for refresh_skip
	 * 0x0000 - get node value updates as soon as they arrive
	 * 0xFFFF - unsubscribe from this key
	 * Any other value of refresh skip indicates that this node
	 * will be retrieved every n client updates
	 */
	public int update_subscription(int refresh_skip) { return this.update_subscription(refresh_skip, false); }
	public int update_subscription(int refresh_skip, boolean set_children) {
		// Save for later usage
		this.__refresh_skip = refresh_skip;

		if(this.__ready) {
			this.__gambezi._update_subscription(this.__key, refresh_skip, set_children);
			return 0;
		}
		else {
			return 1;
		}
	}

	/**
	 * Retrieves all immediate children of this node from the server
	 */
	public int retrieve_children() {
		if(this.__ready) {
			this.__gambezi._request_id(this.__key , "", true, false);
			return 0;
		}
		else {
			final Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.retrieve_children();
				}
			});
			return 1;
		}
	}

	/**
	 * Retrieves all children of this node recursively from the server
	 */
	public int retrieve_children_all() {
		if(this.__ready) {
			this.__gambezi._request_id(this.__key , "", true, true);
			return 0;
		}
		else {
			final Node node = this;
			this.__send_queue.push(new SendQueueItem() {
				public void run() {
					node.retrieve_children_all();
				}
			});
			return 1;
		}
	}

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
		return this.set_data_raw(buf, 0, length);
	}

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

	/**
	 * Sets the value of the node as a boolean
	 */
	public int set_boolean(boolean value) {
		int length = 1;
		byte[] buf = new byte[length];
		buf[0] = (byte)(value ? 0x01 : 0x00);
		return this.set_data_raw(buf, 0, length);
	}

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

	/**
	 * Sets the value of the node as a string
	 */
	public int set_string(String value) {
		byte[] buffer = value.getBytes(StandardCharsets.UTF_8);
		return this.set_data_raw(buffer, 0, buffer.length);
	}

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
