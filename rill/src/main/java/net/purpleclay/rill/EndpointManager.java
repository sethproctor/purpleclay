/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface EndpointManager {

	/** Does this need some credentials, interfaces, etc.? */
	void startServer(ConnectionHandler handler);

	void shutdown();

	/** Should this be here? Or a handle with address, credentials etc.? */
	Endpoint open(InetSocketAddress address) throws IOException;

}
