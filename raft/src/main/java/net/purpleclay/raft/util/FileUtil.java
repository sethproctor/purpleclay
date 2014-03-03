/*
 * Copyright (c) 2013-2014, Seth Proctor. All rights reserved.
 *
 * This software is distributed under the BSD license. See the terms of the
 * license in the documentation provided with this software.
 */

package net.purpleclay.raft.util;

import java.io.File;
import java.io.IOException;


/** Utility routines for file management. */
public abstract class FileUtil {

	/**
	 * If the directory exists this routine validates that the directory is
	 * readable and writable. If the directory does not exist but the parent
	 * directory does then this routine attempts to create the directory.
	 *
	 * @param dir a relative or absolute path to a directory
	 *
	 * @return a {@code File} representing the directory
	 *
	 * @throws IOException if the directory cannot be created or is not usable
	 */
	public static File validateDirectory(String dir) throws IOException {
		File dirFile = new File(dir);

		if (! dirFile.exists()) {
			File parent = dirFile.getParentFile();

			if ((parent == null) || (! parent.isDirectory()))
				throw new IOException("invalid directory");
			if ((! parent.canExecute()) || (! parent.canWrite()))
				throw new IOException("cannot create log directory");
			if (! dirFile.mkdir())
				throw new IOException("failed to create directory");
		}

		if ((! dirFile.canRead()) || (! dirFile.canExecute()))
			throw new IOException("cannot access directory");
		if (! dirFile.canWrite())
			throw new IOException("cannot write to directory");

		return dirFile;
	}

}
