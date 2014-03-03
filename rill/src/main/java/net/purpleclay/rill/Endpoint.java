/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill;

import java.io.IOException;

public interface Endpoint {

	void addListener(EndpointListener listener);

	void removeListener(EndpointListener listener);

	void send(byte [] message) throws IOException;

	boolean send(byte [] message, EndpointListener responseListener);

	byte [] sendAndReceive(byte [] message);

	void close();

}
