/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.swift.exceptions.SwiftNotDirectoryException;
import org.apache.hadoop.fs.swift.exceptions.SwiftOperationFailedException;
import org.apache.hadoop.fs.swift.exceptions.SwiftPathExistsException;
import org.apache.hadoop.fs.swift.exceptions.SwiftUnsupportedFeatureException;
import org.apache.hadoop.fs.swift.util.SwiftObjectPath;
import org.apache.hadoop.fs.swift.util.SwiftUtils;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Swift file system implementation. Extends Hadoop FileSystem
 */
public class SwiftNativeFileSystem extends FileSystem {

  /** filesystem prefix: {@value} */
  public static final String SWIFT = "swift";
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeFileSystem.class);

  /**
   * path to user work directory for storing temporary files
   */
  private Path workingDir;

  /**
   * Swift URI
   */
  private URI uri;

  /**
   * reference to swiftFileSystemStore
   */
  private SwiftNativeFileSystemStore store;

  /**
   * Default constructor for Hadoop
   */
  public SwiftNativeFileSystem() {
    // set client in initialize()
  }

  /**
   * This constructor used for testing purposes
   */
  public SwiftNativeFileSystem(SwiftNativeFileSystemStore store)
          throws IOException {
    this.store = store;
  }

  /**
   * This is for testing
   * @return the inner store class
   */
  public SwiftNativeFileSystemStore getStore() {
    return store;
  }

  @Override
  public String getScheme() {
    return SWIFT;
  }

  /**
   * default class initialization
   *
   * @param fsuri path to Swift
   * @param conf  Hadoop configuration
   * @throws IOException
   */
  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);

    setConf(conf);
    if (store == null) {
      store = new SwiftNativeFileSystemStore();
    }
    this.uri = fsuri;
    this.workingDir = new Path("/user",
            System.getProperty("user.name")).makeQualified(uri, new Path(System.getProperty("user.name")));
    if (LOG.isDebugEnabled()) {
      LOG.debug("Initializing SwiftNativeFileSystem against URI " + uri
              + " and working dir " + workingDir);
    }
    store.initialize(uri, conf);
    LOG.debug("SwiftFileSystem initialized");
  }

  /**
   * @return path to Swift
   */
  @Override
  public URI getUri() {

    return uri;
  }

  @Override
  public String toString() {
    return "SwiftNativeFileSystem " + uri;
  }

  /**
   * Path to user working directory
   *
   * @return Hadoop path
   */
  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * @param dir user working directory
   */
  @Override
  public void setWorkingDirectory(Path dir) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.setWorkingDirectory to " + dir);
    }

    workingDir = dir;
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {

    return store.getObjectMetadata(f);
  }


  @Override
  public boolean isFile(Path f) throws IOException {

    try {
      FileStatus fileStatus = getFileStatus(f);
      return !SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  @Override
  public boolean isDirectory(Path f) throws IOException {

    try {
      FileStatus fileStatus = getFileStatus(f);
      return SwiftUtils.isDirectory(fileStatus);
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * Return an array containing hostnames, offset and size of
   * portions of the given file.  For a nonexistent
   * file or regions, null will be returned.
   * <p/>
   * This call is most helpful with DFS, where it returns
   * hostnames of machines that contain the given file.
   * <p/>
   * The FileSystem will simply return an elt containing 'localhost'.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file,
                                               long start,
                                               long len) throws IOException {
    //argument checks
    if (file == null) {
      return null;
    }

    if (start < 0 || len < 0) {
      throw new IllegalArgumentException("Negative start or len parameter" +
                                         " to getFileBlockLocations");
    }
    if (file.getLen() <= start) {
      return new BlockLocation[0];
    }

    // Check if requested file in Swift is more than 5Gb. In this case
    // each block has its own location -which may be determinable
    // from the Swift client API, depending on the remote server
    final FileStatus[] listOfFileBlocks = store.listSubPaths(file.getPath());
    List<URI> locations = new ArrayList<URI>();
    if (listOfFileBlocks.length > 1) {
      for (FileStatus fileStatus : listOfFileBlocks) {
        if (SwiftObjectPath.fromPath(uri, fileStatus.getPath())
                .equals(SwiftObjectPath.fromPath(uri, file.getPath()))) {
          continue;
        }
        locations.addAll(store.getObjectLocation(fileStatus.getPath()));
      }
    } else {
      locations = store.getObjectLocation(file.getPath());
    }

    if (locations.isEmpty()) {
      LOG.debug("No locations returned for " + file.getPath());
      //no locations were returned for the object
      //fall back to the superclass
      return super.getFileBlockLocations(file, start, len);
    }

    final String[] names = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    int i = 0;
    for (URI location : locations) {
      hosts[i] = location.getHost();
      names[i] = location.getAuthority();
      i++;
    }
    return new BlockLocation[]{
            new BlockLocation(names, hosts, 0, file.getLen())
    };
  }

  /**
   * Create the parent directories.
   * As an optimization, the entire hierarchy of parent
   * directories is <i>Not</i> polled. Instead
   * the tree is walked up from the last to the first,
   * creating directories until one that exists is found.
   * 
   * This strategy means if a file is created in an existing directory,
   * one quick poll sufficies.
   * 
   * There is a big assumption here: that all parent directories of an existing
   * directory also exists. 
   * @param path path to create.
   * @param permission to apply to files
   * @return true if the operation was successful
   * @throws IOException on a problem
   */
  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.mkdirs: " + path);
    }
    Path directory = makeAbsolute(path);

    //build a list of paths to create
    List<Path> paths = new ArrayList<Path>();
    while (shouldCreate(directory)) {
      //this directory needs creation, add to the list
      paths.add(0, directory);
      //now see if the parent needs to be created
      directory = directory.getParent();
    }

    //go through the list of directories to create
    for (Path p : paths) {
      if (isNotRoot(p)) {
        //perform a mkdir operation without any polling of
        //the far end first
        forceMkdir(p);
      }
    }
    
    //if an exception was not thrown, this operation is considered
    //a success
    return true;
  }

  private boolean isNotRoot(Path absolutePath) {
    return !isRoot(absolutePath);
  }
  
  private boolean isRoot(Path absolutePath) {
    return absolutePath.getParent() == null;
  }


  /**
   * internal implementation of directory creation.
   *
   * @param path path to file
   * @return boolean file is created; false: no need to create
   * @throws IOException if specified path is file instead of directory
   */
  private boolean mkdir(Path path) throws IOException {
    Path directory = makeAbsolute(path);
    boolean shouldCreate = shouldCreate(directory);
    if (shouldCreate) {
      forceMkdir(directory);
    }
    return shouldCreate;
  }

  /**
   * Should mkdir create this directory. 
   * If the directory is root : false
   * If the entry exists and is a directory: false
   * If the entry exists and is a file: exception
   * else: true
   * @param directory path to query
   * @return true iff the directory should be created
   * @throws IOException IO problems
   * @throws SwiftNotDirectoryException if the path references a file
   */
  private boolean shouldCreate(Path directory) throws IOException {
    FileStatus fileStatus;
    boolean shouldCreate;
    if (isRoot(directory)) {
      //its the base dir, bail out immediately
      return false;
    }
    try {
      //find out about the path
      fileStatus = getFileStatus(directory);
      
      if (!SwiftUtils.isDirectory(fileStatus)) {
        //if it's a file, raise an error
        throw new SwiftNotDirectoryException(directory,
                String.format(": can't mkdir since it exists and is not a directory: %s",
                        fileStatus));
      } else {
        //path exists, and it is a directory
        if (LOG.isDebugEnabled()) {
          LOG.debug("skipping mkdir(" + directory + ") as it exists already");
        }
        shouldCreate = false;
      }
    } catch (FileNotFoundException e) {
      shouldCreate = true;
    }
    return shouldCreate;
  }

  /**
   * mkdir of a directory -irrespective of what was there underneath.
   * There are no checks for the directory existing, there not
   * being a path there, etc. etc. Those are assumed to have
   * taken place already
   * @param absolutePath path to create
   * @throws IOException IO problems
   */
  private void forceMkdir(Path absolutePath) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Making dir '" + absolutePath + "' in Swift");
    }
    //file is not found: it must be created
    store.createDirectory(absolutePath);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given path
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("SwiftFileSystem.listStatus for: " + f);
    }
    return store.listSubPaths(f);
  }

  /**
   * This optional operation is not supported yet
   */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws
          IOException {
    LOG.debug("SwiftFileSystem.append");
    throw new SwiftUnsupportedFeatureException("Not supported: append()");
  }

  /**
   * @param permission Currently ignored.
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
                                   boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress)
          throws IOException {
    LOG.debug("SwiftFileSystem.create");

    FileStatus fileStatus = null;
    try {
      fileStatus = getFileStatus(makeAbsolute(file));
    } catch (FileNotFoundException e) {
      //nothing to do
    }

    if (fileStatus != null && !SwiftUtils.isDirectory(fileStatus)) {
      if (overwrite) {
        delete(file, true);
      } else {
        throw new SwiftPathExistsException("File already exists: " + file);
      }
    } else {
      Path parent = file.getParent();
      if (parent != null) {
        if (!mkdirs(parent)) {
          throw new SwiftOperationFailedException("Mkdirs failed to create " + parent);
        }
      }
    }

    SwiftNativeOutputStream out = new SwiftNativeOutputStream(getConf(),
            store,
            file.toUri()
                    .toString());
    return new FSDataOutputStream(out, statistics);
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param path       the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return new FSDataInputStream(
            new BufferedFSInputStream(
                    new SwiftNativeInputStream(store, statistics, path), bufferSize));
  }

  /**
   * Renames Path src to Path dst. On swift this uses copy-and-delete
   * and <i>is not atomic</i>.
   *
   * @param src path
   * @param dst path
   * @return true if directory renamed, false otherwise
   * @throws IOException on problems
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    try {
      store.rename(makeAbsolute(src), makeAbsolute(dst));
      //success
      return true;
    } catch (SwiftOperationFailedException e) {
      //downgrade to a failure
      return false;
    } catch (FileNotFoundException e) {
      //downgrade to a failure
      return false;
    }
  }


  /**
   * Delete a file or directory
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException IO problems
   */
  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      return innerDelete(path, recursive);
    } catch (FileNotFoundException e) {
      //base path was not found.
      return false;
    }
  }

  /**
   * Delete a file.
   * This method is abstract in Hadoop 1.x; in 2.x+ it is non-abstract
   * and deprecated
   */
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  /**
   * Delete the entire tree. This is an internal one with slightly different
   * behavior: if an entry is missing, a {@link FileNotFoundException} is
   * raised. This lets the caller distinguish a file not found with
   * other reasons for failure, so handles race conditions in recursive
   * directory deletes better.
   * <p/>
   * The problem being addressed is: caller A requests a recursive directory
   * of directory /dir ; caller B requests a delete of a file /dir/file,
   * between caller A enumerating the files contents, and requesting a delete
   * of /dir/file. We want to recognise the special case
   * "directed file is no longer there" and not convert that into a failure
   *
   * @param path      the path to delete.
   * @param recursive if path is a directory and set to
   *                  true, the directory is deleted else throws an exception if the
   *                  directory is not empty
   *                  case of a file the recursive can be set to either true or false.
   * @return true if the object was deleted
   * @throws IOException           IO problems
   * @throws FileNotFoundException if a file/dir being deleted is not there -
   *                               this includes entries below the specified path, (if the path is a dir
   *                               and recursive is true)
   */
  private boolean innerDelete(Path path, boolean recursive) throws IOException {
    Path absolutePath = makeAbsolute(path);
    final FileStatus fileStatus;
    fileStatus = getFileStatus(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting path '" + path + "'");
    }
    if (!SwiftUtils.isDirectory(fileStatus)) {
      //simple file: delete it
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting simple file '" + path + "'");
      }
      store.deleteObject(absolutePath);
    } else {
      //it's a directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting directory '" + path + "'");
      }
      FileStatus[] contents = listStatus(absolutePath);
      if (contents == null) {
        //the directory went away during the non-atomic stages of the operation.
        // Return false as it was not this thread doing the deletion.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Path '" + path + "' has no status -it has 'gone away'");
        }
        return false;
      }
      //now build a list without ourselves in it
      Path dirPath = fileStatus.getPath();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found " + contents.length + " child entries under " + dirPath);
      }
      ArrayList<FileStatus> children =
              new ArrayList<FileStatus>(contents.length);
      for (FileStatus child : contents) {
        if (!(child.getPath().equals(dirPath))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(child.toString());
          }
          children.add(child);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("skipping own entry");
          }
        }
      }

      //look to see if there are now any children
      if (!children.isEmpty() && !recursive) {
        //if there are children, unless this is a recursive operation, fail immediately
        throw new SwiftOperationFailedException("Directory " + path + " is not empty.");
      }
      //delete the children
      for (FileStatus child : children) {
        Path childPath = child.getPath();
        try {
          if (!innerDelete(childPath, true)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to recursively delete '" + childPath + "'");
            }
            return false;
          }
        } catch (FileNotFoundException e) {
          //the path went away -race conditions.
          //do not fail, as the outcome is still OK.
          LOG.info("Path " + childPath + " is no longer present");
        }
      }
      //here any children that existed have been deleted
      //so rm the directory
      store.rmdir(absolutePath);
    }

    return true;
  }

  /**
   * Makes path absolute
   *
   * @param path path to file
   * @return absolute path
   */
  protected Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }
}
