/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.rill.server;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.purpleclay.rill.Endpoint;
import net.purpleclay.rill.EndpointStreamHandler;


class DefaultStreamHandler implements EndpointStreamHandler {

	@Override public byte [] handleInput(Endpoint endpoint, ByteBuffer input)
		throws IOException
	{
		// TODO: for now, assume that all messages are self-contained, but
		// in practice this should be bundling up message details

		int msgLen = input.getInt();

		if (input.remaining() != msgLen)
			throw new IOException("wrong amount of data available: " + input.remaining() + ", " + msgLen);

		byte [] message = new byte[msgLen];
		input.get(message);
		return message;
	}

	@Override public void encodeOutput(byte [] input, ByteBuffer output) {
		output.putInt(input.length);
		output.put(input);
	}

}
