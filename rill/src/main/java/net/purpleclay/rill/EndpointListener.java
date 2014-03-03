/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill;

public interface EndpointListener {

	void messageReceived(byte [] message);

	void disconnected();

}
