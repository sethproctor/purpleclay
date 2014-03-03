/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface EndpointStreamHandler {

	byte [] handleInput(Endpoint endpoint, ByteBuffer input) throws IOException;

	void encodeOutput(byte [] input, ByteBuffer output);

}
