/** \ingroup rpmio
 * \file rpmio/mongo.c
 */

/*    Copyright 2009, 2010 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "system.h"

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>

#include <rpmiotypes.h>
#include <rpmio.h>	/* for *Pool methods */
#include <rpmlog.h>
#include <rpmurl.h>

#define	_RPMMGO_INTERNAL
#include <mongo.h>

#include "debug.h"

/*@unchecked@*/
int _rpmmgo_debug = 0;

/*@unchecked@*/ /*@relnull@*/
rpmmgo _rpmmgoI;

/* only need one of these */
static const int zero = 0;
static const int one = 1;

/*==============================================================*/
/* --- gridfs.h */

enum {DEFAULT_CHUNK_SIZE = 256 * 1024};

typedef uint64_t gridfs_offset;

/* A GridFS represents a single collection of GridFS files in the database. */
typedef struct {
    mongo *client; /**> The client to db-connection. */
    const char *dbname; /**> The root database name */
    const char *prefix; /**> The prefix of the GridFS's collections, default is NULL */
    const char *files_ns; /**> The namespace where the file's metadata is stored */
    const char *chunks_ns; /**. The namespace where the files's data is stored in chunks */
    bson_bool_t caseInsensitive; /**. If true then files are matched in case insensitive fashion */
} gridfs;

/* A GridFile is a single GridFS file. */
typedef struct {
    gridfs *gfs;        /**> The GridFS where the GridFile is located */
    bson *meta;         /**> The GridFile's bson object where all its metadata is located */
    gridfs_offset pos;  /**> The position is the offset in the file */
    bson_oid_t id;      /**> The files_id of the gridfile */
    char *remote_name;  /**> The name of the gridfile as a string */
    char *content_type; /**> The gridfile's content type */
    gridfs_offset length; /**> The length of this gridfile */
    int chunk_num;      /**> The number of the current chunk being written to */
    char *pending_data; /**> A buffer storing data still to be written to chunks */
    size_t pending_len;    /**> Length of pending_data buffer */
    int flags;          /**> Store here special flags such as: No MD5 calculation and Zlib Compression enabled*/
    int chunkSize;   /**> Let's cache here the cache size to avoid accesing it on the Meta mongo object every time is needed */
} gridfile;

enum gridfile_storage_type {
    GRIDFILE_DEFAULT = 0,
    GRIDFILE_NOMD5 = ( 1<<0 )
};

#ifndef _MSC_VER
static char *_strupr(char *str);
#ifdef	UNUSED
static char *_strlwr(char *str);
#endif
#endif

typedef int ( *gridfs_chunk_filter_func )( char** targetBuf, size_t* targetLen, const char* srcBuf, size_t srcLen, int flags );
typedef size_t ( *gridfs_pending_data_size_func ) (int flags);

MONGO_EXPORT gridfs* gridfs_alloc( void );
MONGO_EXPORT void gridfs_dealloc(gridfs* gfs);
MONGO_EXPORT gridfile* gridfile_create( void );
MONGO_EXPORT void gridfile_dealloc(gridfile* gf);
MONGO_EXPORT void gridfile_get_descriptor(gridfile* gf, bson* out);
MONGO_EXPORT void gridfs_set_chunk_filter_funcs(gridfs_chunk_filter_func writeFilter, gridfs_chunk_filter_func readFilter, gridfs_pending_data_size_func pendingDataNeededSize);

/**
 *  Initializes a GridFS object
 *  @param client - db connection
 *  @param dbname - database name
 *  @param prefix - collection prefix, default is fs if NULL or empty
 *  @param gfs - the GridFS object to initialize
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfs_init( mongo *client, const char *dbname,
                              const char *prefix, gridfs *gfs );

/**
 * Destroys a GridFS object. Call this when finished with
 * the object..
 *
 * @param gfs a grid
 */
MONGO_EXPORT void gridfs_destroy( gridfs *gfs );

/**
 *  Initializes a GridFile containing the GridFS and file bson
 *  @param gfs - the GridFS where the GridFile is located
 *  @param meta - the file object
 *  @param gfile - the output GridFile that is being initialized
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfile_init( gridfs *gfs, const bson *meta, gridfile *gfile );

/**
 *  Destroys the GridFile
 *
 *  @param gfile - the GridFile being destroyed
 */
MONGO_EXPORT void gridfile_destroy( gridfile *gfile );

/**
 *  Initializes a gridfile for writing incrementally with gridfs_write_buffer.
 *  Once initialized, you can write any number of buffers with gridfs_write_buffer.
 *  When done, you must call gridfs_writer_done to save the file metadata.
 *  +-+-+-+-  This modified version of GridFS allows the file to read/write randomly
 *  +-+-+-+-  when using this function
 *
 *  @param gfile - the GridFile
 *  @param gfs - the working GridFS
 *  @param remote_name - filename for use in the database
 *  @param content_type - optional MIME type for this object
 *  @param flags - flags
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfile_writer_init( gridfile *gfile, gridfs *gfs, const char *remote_name,
                                       const char *content_type, int flags );

/**
 *  Write to a GridFS file incrementally. You can call this function any number
 *  of times with a new buffer each time. This allows you to effectively
 *  stream to a GridFS file. When finished, be sure to call gridfs_writer_done.
 * 
 *  @param gfile - GridFile to write to
 *  @param data - Pointer to buffer with data to be written
 *  @param length - Size of buffer with data to be written
 *  @return - Number of bytes written. If different from length assume somethind went wrong
 */
MONGO_EXPORT gridfs_offset gridfile_write_buffer( gridfile *gfile, const char *data, gridfs_offset length );

/**
 *  Signal that writing of this gridfile is complete by
 *  writing any buffered chunks along with the entry in the
 *  files collection.
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfile_writer_done( gridfile *gfile );

/**
 *  Store a buffer as a GridFS file.
 *  @param gfs - the working GridFS
 *  @param data - pointer to buffer to store in GridFS
 *  @param length - length of the buffer
 *  @param remotename - filename for use in the database
 *  @param contenttype - optional MIME type for this object
 *  @param flags - flags
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfs_store_buffer( gridfs *gfs, const char *data, gridfs_offset length,
                          const char *remotename,
                          const char *contenttype, int flags );

/**
 *  Open the file referenced by filename and store it as a GridFS file.
 *  @param gfs - the working GridFS
 *  @param filename - local filename relative to the process
 *  @param remotename - optional filename for use in the database
 *  @param contenttype - optional MIME type for this object
 *
 *  @return - MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfs_store_file( gridfs *gfs, const char *filename,
                        const char *remotename, const char *contenttype, int flags );

/**
 *  Removes the files referenced by filename from the db
 *
 *  @param gfs - the working GridFS
 *  @param filename - the filename of the file/s to be removed
 *
 *  @return MONGO_OK if a matching file was removed, and MONGO_ERROR if
 *    an error occurred or the file did not exist
 */
MONGO_EXPORT int gridfs_remove_filename( gridfs *gfs, const char *filename );

/**
 *  Find the first file matching the provided query within the
 *  GridFS files collection, and return the file as a GridFile.
 *
 *  @param gfs - the working GridFS
 *  @param query - a pointer to the bson with the query data
 *  @param gfile - the output GridFile to be initialized
 *
 *  @return MONGO_OK if successful, MONGO_ERROR otherwise
 */
MONGO_EXPORT int gridfs_find_query( gridfs *gfs, const bson *query, gridfile *gfile );

/**
 *  Find the first file referenced by filename within the GridFS
 *  and return it as a GridFile
 *  @param gfs - the working GridFS
 *  @param filename - filename of the file to find
 *  @param gfile - the output GridFile to be intialized
 *
 *  @return MONGO_OK or MONGO_ERROR.
 */
MONGO_EXPORT int gridfs_find_filename( gridfs *gfs, const char *filename, gridfile *gfile );

/**
 *  Returns whether or not the GridFile exists
 *  @param gfile - the GridFile being examined
 */
MONGO_EXPORT bson_bool_t gridfile_exists( const gridfile *gfile )
	RPM_GNUC_PURE;

/**
 *  Returns the filename of GridFile
 *  @param gfile - the working GridFile
 *
 *  @return - the filename of the Gridfile
 */
MONGO_EXPORT const char *gridfile_get_filename( const gridfile *gfile );

/**
 *  Returns the size of the chunks of the GridFile
 *  @param gfile - the working GridFile
 *
 *  @return - the size of the chunks of the Gridfile
 */
MONGO_EXPORT int gridfile_get_chunksize( const gridfile *gfile );

/**
 *  Returns the length of GridFile's data
 *
 *  @param gfile - the working GridFile
 *
 *  @return - the length of the Gridfile's data
 */
MONGO_EXPORT gridfs_offset gridfile_get_contentlength( const gridfile *gfile );

/**
 *  Returns the MIME type of the GridFile
 *
 *  @param gfile - the working GridFile
 *
 *  @return - the MIME type of the Gridfile
 *            (NULL if no type specified)
 */
MONGO_EXPORT const char *gridfile_get_contenttype( const gridfile *gfile );

/**
 *  Returns the upload date of GridFile
 *
 *  @param gfile - the working GridFile
 *
 *  @return - the upload date of the Gridfile
 */
MONGO_EXPORT bson_date_t gridfile_get_uploaddate( const gridfile *gfile );

/**
 *  Returns the MD5 of GridFile
 *
 *  @param gfile - the working GridFile
 *
 *  @return - the MD5 of the Gridfile
 */
MONGO_EXPORT const char *gridfile_get_md5( const gridfile *gfile );

/**
 *  Returns the _id in GridFile specified by name
 *
 *  @param gfile - the working GridFile
 * 
 *  @return - the _id field in metadata
 */
MONGO_EXPORT bson_oid_t gridfile_get_id( const gridfile *gfile );

/**
 *  Returns the field in GridFile specified by name
 *
 *  @param gfile - the working GridFile
 *  @param name - the name of the field to be returned
 *
 *  @return - the data of the field specified
 *            (NULL if none exists)
 */
MONGO_EXPORT const char *gridfile_get_field( gridfile *gfile,
                                             const char *name );

/**
 *  Returns the caseInsensitive flag value of gfs
 *  @param gfs - the working gfs
 *
 *  @return - the caseInsensitive flag of the gfs
 */
MONGO_EXPORT bson_bool_t gridfs_get_caseInsensitive( const gridfs *gfs )
	RPM_GNUC_PURE;

/**
 *  Sets the caseInsensitive flag value of gfs
 *  @param gfs - the working gfs
 *  @param newValue - the new value for the caseInsensitive flag of gfs
 *
 */
MONGO_EXPORT void gridfs_set_caseInsensitive(gridfs *gfs, bson_bool_t newValue);

/**
 *  Sets the flags of the GridFile
 *  @param gfile - the working GridFile
 *  @param flags - the value of the flags to set on the provided GridFile
 *
 */
MONGO_EXPORT void gridfile_set_flags(gridfile *gfile, int flags);

/**
 *  gets the flags of the GridFile
 *  @param gfile - the working GridFile
 *
 */
MONGO_EXPORT int gridfile_get_flags( const gridfile *gfile )
	RPM_GNUC_PURE;

/**
 *  Returns a boolean field in GridFile specified by name
 *  @param gfile - the working GridFile
 *  @param name - the name of the field to be returned
 *
 *  @return - the boolean of the field specified
 *            (NULL if none exists)
 */
MONGO_EXPORT bson_bool_t gridfile_get_boolean( const gridfile *gfile,
                                  const char *name );

/**
 *  Returns the metadata of GridFile. Calls bson_init_empty on metadata
 *  if none exits.
 *
 * @note When copyData is false, the metadata object becomes invalid
 *       when gfile is destroyed. For either value of copyData, you
 *       must pass the metadata object to bson_destroy when you are
 *       done using it.
 *
 *  @param gfile - the working GridFile
 *  @param metadata an uninitialized BSON object to receive the metadata.
 *  @param copyData when true, makes a copy of the scope data which will remain
 *    valid when the grid file is deallocated.
 */
MONGO_EXPORT void gridfile_get_metadata( const gridfile *gfile, bson* metadata, bson_bool_t copyData );

/**
 *  Returns the number of chunks in the GridFile
 *  @param gfile - the working GridFile
 *
 *  @return - the number of chunks in the Gridfile
 */
MONGO_EXPORT int gridfile_get_numchunks( const gridfile *gfile );

/**
 *  Returns chunk n of GridFile
 *  @param gfile - the working GridFile
 *  @param n - chunk n
 *  @retval out - (*out) the BSON result
 *
 */
MONGO_EXPORT void gridfile_get_chunk( gridfile *gfile, int n, bson* out );

/**
 *  Returns a mongo_cursor of *size* chunks starting with chunk *start*
 *
 *  @param gfile - the working GridFile
 *  @param start - the first chunk in the cursor
 *  @param size - the number of chunks to be returned
 *
 *  @return - mongo_cursor of the chunks (must be destroyed after use)
 */
MONGO_EXPORT mongo_cursor *gridfile_get_chunks( gridfile *gfile, size_t start, size_t size );

/**
 *  Writes the GridFile to a stream
 *
 *  @param gfile - the working GridFile
 *  @param stream - the file stream to write to
 */
MONGO_EXPORT gridfs_offset gridfile_write_file( gridfile *gfile, FILE *stream );

/**
 *  Reads length bytes from the GridFile to a buffer
 *  and updates the position in the file.
 *  (assumes the buffer is large enough)
 *  (if size is greater than EOF gridfile_read reads until EOF)
 *
 *  @param gfile - the working GridFile
 *  @param size - the amount of bytes to be read
 *  @param buf - the buffer to read to
 *
 *  @return - the number of bytes read
 */
MONGO_EXPORT gridfs_offset gridfile_read_buffer( gridfile *gfile, char *buf, gridfs_offset size );

/**
 *  Updates the position in the file
 *  (If the offset goes beyond the contentlength,
 *  the position is updated to the end of the file.)
 *
 *  @param gfile - the working GridFile
 *  @param offset - the position to update to
 *
 *  @return - resulting offset location
 */
MONGO_EXPORT gridfs_offset gridfile_seek( gridfile *gfile, gridfs_offset offset );

/**
 *  @param gfile - the working GridFile
 *  @param newSize - the new size after truncation
 *
 */
MONGO_EXPORT gridfs_offset gridfile_truncate(gridfile *gfile, gridfs_offset newSize);

/**
 *  @param gfile - the working GridFile
 *  @param bytesToExpand - number of bytes the file will be expanded
 *
 */
MONGO_EXPORT gridfs_offset gridfile_expand(gridfile *gfile, gridfs_offset bytesToExpand);

/**
 *  @param gfile - the working GridFile
 *  @param newSize - the new size of file
 *
 */
MONGO_EXPORT gridfs_offset gridfile_set_size(gridfile *gfile, gridfs_offset newSize);

/*==============================================================*/
/* --- gridfs.c */

#ifndef _MSC_VER
static char *_strupr(char *str)
{
   char *s = str;
   while (*s) {
        *s = toupper((unsigned char)*s);
        ++s;
      }
   return str;
}
#ifdef	UNUSED
static char *_strlwr(char *str)
{
   char *s = str;
   while (*s) {
        *s = tolower((unsigned char)*s);
        ++s;
   }
   return str;
}
#endif
#endif

/* Memory allocation functions */
MONGO_EXPORT gridfs *gridfs_alloc( void ) {
  return ( gridfs* )bson_malloc( sizeof( gridfs ) );
}

MONGO_EXPORT void gridfs_dealloc( gridfs *gfs ) {
  bson_free( gfs );
}

MONGO_EXPORT gridfile *gridfile_create( void ) {
  gridfile* gfile = (gridfile*)bson_malloc(sizeof(gridfile));  
  memset( gfile, 0, sizeof ( gridfile ) );  
  return gfile;
}

MONGO_EXPORT void gridfile_dealloc( gridfile *gf ) {
  bson_free( gf );
}

MONGO_EXPORT void gridfile_get_descriptor(gridfile *gf, bson *out) {
  *out =  *gf->meta;
}

/* Default chunk pre and post processing logic */
static int gridfs_default_chunk_filter(char** targetBuf, size_t* targetLen, const char* srcData, size_t srcLen, int flags) {
  *targetBuf = (char *) srcData;
  *targetLen = srcLen;
  return 0;
}

static size_t gridfs_default_pending_data_size (int flags) {
  return DEFAULT_CHUNK_SIZE;
}
/* End of default functions for chunks pre and post processing */

static gridfs_chunk_filter_func gridfs_write_filter = gridfs_default_chunk_filter;
static gridfs_chunk_filter_func gridfs_read_filter = gridfs_default_chunk_filter;
static gridfs_pending_data_size_func gridfs_pending_data_size = gridfs_default_pending_data_size;

MONGO_EXPORT void gridfs_set_chunk_filter_funcs(gridfs_chunk_filter_func writeFilter, gridfs_chunk_filter_func readFilter, gridfs_pending_data_size_func pendingDataNeededSize) {
  gridfs_write_filter = writeFilter;
  gridfs_read_filter = readFilter;
  gridfs_pending_data_size = pendingDataNeededSize; 
}

static bson *chunk_new(bson_oid_t id, int chunkNumber, char** dataBuf, const char* srcData, size_t len, int flags ) {
  bson *b = bson_alloc();
  size_t dataBufLen = 0;

  if( gridfs_write_filter( dataBuf, &dataBufLen, srcData, len, flags) != 0 ) {
    return NULL;
  }
  bson_init_size(b, (int) dataBufLen + 128); /* a little space for field names, files_id, and n */
  bson_append_oid(b, "files_id", &id);
  bson_append_int(b, "n", chunkNumber);
  bson_append_binary(b, "data", BSON_BIN_BINARY, *dataBuf, (int)dataBufLen);
  bson_finish(b);
  return b;
}

static void chunk_free(bson *oChunk) {
  if( oChunk ) {
    bson_destroy(oChunk);
    bson_dealloc(oChunk);
  }
}
/* End of memory allocation functions */

/* -------------- */
/* gridfs methods */
/* -------------- */

/* gridfs constructor */
MONGO_EXPORT int gridfs_init(mongo *client, const char *dbname, const char *prefix, gridfs *gfs) {

  bson b;

  gfs->caseInsensitive = 0;
  gfs->client = client;

  /* Allocate space to own the dbname */
  gfs->dbname = (const char*)bson_malloc((int)strlen(dbname) + 1);
  strcpy((char*)gfs->dbname, dbname);

  /* Allocate space to own the prefix */
  if (prefix == NULL) {
    prefix = "fs";
  }
  gfs->prefix = (const char*)bson_malloc((int)strlen(prefix) + 1);
  strcpy((char*)gfs->prefix, prefix);

  /* Allocate space to own files_ns */
  gfs->files_ns = (const char*)bson_malloc((int)(strlen(prefix) + strlen(dbname) + strlen(".files") + 2));
  strcpy((char*)gfs->files_ns, dbname);
  strcat((char*)gfs->files_ns, ".");
  strcat((char*)gfs->files_ns, prefix);
  strcat((char*)gfs->files_ns, ".files");

  /* Allocate space to own chunks_ns */
  gfs->chunks_ns = (const char*)bson_malloc((int)(strlen(prefix) + strlen(dbname) + strlen(".chunks") + 2));
  strcpy((char*)gfs->chunks_ns, dbname);
  strcat((char*)gfs->chunks_ns, ".");
  strcat((char*)gfs->chunks_ns, prefix);
  strcat((char*)gfs->chunks_ns, ".chunks");

  bson_init(&b);
  bson_append_int(&b, "filename", 1);
  bson_finish(&b);
  if( mongo_create_index(gfs->client, gfs->files_ns, &b, NULL, 0, NULL) != MONGO_OK) {
    bson_destroy( &b );
    gridfs_destroy( gfs );
    return MONGO_ERROR;
  }
  bson_destroy(&b);

  bson_init(&b);
  bson_append_int(&b, "files_id", 1);
  bson_append_int(&b, "n", 1);
  bson_finish(&b);
  if( mongo_create_index(gfs->client, gfs->chunks_ns, &b, NULL, MONGO_INDEX_UNIQUE, NULL) != MONGO_OK ) {
    bson_destroy(&b);
    gridfs_destroy( gfs );    
    return MONGO_ERROR;
  }
  bson_destroy(&b);

  return MONGO_OK;
}

/* gridfs destructor */
MONGO_EXPORT void gridfs_destroy(gridfs *gfs) {
  if( gfs == NULL ) return;
  if( gfs->dbname ) {
    bson_free((char*)gfs->dbname);
    gfs->dbname = NULL;
  }
  if( gfs->prefix ) {
    bson_free((char*)gfs->prefix);
    gfs->prefix = NULL;
  }
  if( gfs->files_ns ) {
    bson_free((char*)gfs->files_ns);
    gfs->files_ns = NULL;
  }
  if( gfs->chunks_ns ) {
    bson_free((char*)gfs->chunks_ns);
    gfs->chunks_ns = NULL;
  }      
}

/* gridfs accesors */

MONGO_EXPORT bson_bool_t gridfs_get_caseInsensitive( const gridfs *gfs ) {
  return gfs->caseInsensitive;
}

MONGO_EXPORT void gridfs_set_caseInsensitive(gridfs *gfs, bson_bool_t newValue){
  gfs->caseInsensitive = newValue;
}

static int bson_append_string_uppercase( bson *b, const char *name, const char *str, bson_bool_t upperCase ) {
  char *strUpperCase;
  if ( upperCase ) {
    int res; 
    strUpperCase = (char *) bson_malloc( (int) strlen( str ) + 1 );
    strcpy(strUpperCase, str);
    _strupr(strUpperCase);
    res = bson_append_string( b, name, strUpperCase );
    bson_free( strUpperCase );
    return res;
  } else {
    return bson_append_string( b, name, str );
  }
}

static int gridfs_insert_file(gridfs *gfs, const char *name, const bson_oid_t id, gridfs_offset length, const char *contenttype, int flags, int chunkSize) {
  bson command[1];
  bson ret[1];
  bson res[1];
  bson_iterator it[1];
  bson q[1];
  int result;
  int64_t d;

  /* If you don't care about calculating MD5 hash for a particular file, simply pass the GRIDFILE_NOMD5 value on the flag param */
  if( !( flags & GRIDFILE_NOMD5 ) ) {  
    /* Check run md5 */
    bson_init(command);
    bson_append_oid(command, "filemd5", &id);
    bson_append_string(command, "root", gfs->prefix);
    bson_finish(command);
    result = mongo_run_command(gfs->client, gfs->dbname, command, res);
    bson_destroy(command);
    if (result != MONGO_OK) 
      return result;
  } 

  /* Create and insert BSON for file metadata */
  bson_init(ret);
  bson_append_oid(ret, "_id", &id);
  if (name != NULL &&  *name != '\0') {
    bson_append_string_uppercase( ret, "filename", name, gfs->caseInsensitive );
  }
  bson_append_long(ret, "length", length);
  bson_append_int(ret, "chunkSize", chunkSize);
  d = (bson_date_t)1000 * time(NULL);
  bson_append_date(ret, "uploadDate", d);
  if( !( flags & GRIDFILE_NOMD5 ) ) {
    if (bson_find(it, res, "md5") != BSON_EOO )
        bson_append_string(ret, "md5", bson_iterator_string(it));
    else
        bson_append_string(ret, "md5", ""); 
    bson_destroy(res);
  } else {
    bson_append_string(ret, "md5", ""); 
  } 
  if (contenttype != NULL &&  *contenttype != '\0') {
    bson_append_string(ret, "contentType", contenttype);
  }
  if ( gfs->caseInsensitive ) {
    if (name != NULL &&  *name != '\0') {
      bson_append_string(ret, "realFilename", name);
    }
  }
  bson_append_int(ret, "flags", flags);
  bson_finish(ret);

  bson_init(q);
  bson_append_oid(q, "_id", &id);
  bson_finish(q);

  result = mongo_update(gfs->client, gfs->files_ns, q, ret, MONGO_UPDATE_UPSERT, NULL);

  bson_destroy(ret);
  bson_destroy(q);
  
  return result;
}

MONGO_EXPORT int gridfs_store_buffer(gridfs *gfs, const char *data, gridfs_offset length, const char *remotename, const char *contenttype, int flags ) {
  gridfile gfile;
  gridfs_offset bytes_written;
  
  gridfile_init( gfs, NULL, &gfile );
  gridfile_writer_init( &gfile, gfs, remotename, contenttype, flags );
  
  bytes_written = gridfile_write_buffer( &gfile, data, length );

  gridfile_writer_done( &gfile );
  gridfile_destroy( &gfile );

  return bytes_written == length ? MONGO_OK : MONGO_ERROR;
}

MONGO_EXPORT int gridfs_store_file(gridfs *gfs, const char *filename, const char *remotename, const char *contenttype, int flags ) {
  char buffer[DEFAULT_CHUNK_SIZE];
  FILE *fd;    
  gridfs_offset chunkLen;
  gridfile gfile;
  gridfs_offset bytes_written = 0;

  /* Open the file and the correct stream */
  if (strcmp(filename, "-") == 0) {
    fd = stdin;
  } else {
    fd = fopen(filename, "rb");
    if (fd == NULL) {
      return MONGO_ERROR;
    } 
  }

  /* Optional Remote Name */
  if (remotename == NULL ||  *remotename == '\0') {
    remotename = filename;
  }

  if( gridfile_init( gfs, NULL, &gfile ) != MONGO_OK ) return MONGO_ERROR;
  if( gridfile_writer_init( &gfile, gfs, remotename, contenttype, flags ) != MONGO_OK ){
    gridfile_destroy( &gfile );
    return MONGO_ERROR; 
  }

  chunkLen = fread(buffer, 1, DEFAULT_CHUNK_SIZE, fd);
  while( chunkLen != 0 ) {
    bytes_written = gridfile_write_buffer( &gfile, buffer, chunkLen );
    if( bytes_written != chunkLen ) break;
    chunkLen = fread(buffer, 1, DEFAULT_CHUNK_SIZE, fd);
  }

  gridfile_writer_done( &gfile );
  gridfile_destroy( &gfile );

  /* Close the file stream */
  if ( fd != stdin ) {
    fclose( fd );
  }   
  return ( chunkLen == 0) || ( bytes_written == chunkLen ) ? MONGO_OK : MONGO_ERROR;  
}

MONGO_EXPORT int gridfs_remove_filename(gridfs *gfs, const char *filename) {
  bson query[1];
  mongo_cursor *files;
  bson file[1];
  bson_iterator it[1];
  bson_oid_t id;
  bson b[1];
  int ret = MONGO_ERROR;

  bson_init(query);
  bson_append_string_uppercase( query, "filename", filename, gfs->caseInsensitive );
  bson_finish(query);
  files = mongo_find(gfs->client, gfs->files_ns, query, NULL, 0, 0, 0);
  bson_destroy(query);

  /* files should be a valid cursor even if the file doesn't exist */
  if ( files == NULL ) return MONGO_ERROR; 

  /* Remove each file and it's chunks from files named filename */
  while (mongo_cursor_next(files) == MONGO_OK) {
    *file = files->current;
    bson_find(it, file, "_id");
    id =  *bson_iterator_oid(it);

    /* Remove the file with the specified id */
    bson_init(b);
    bson_append_oid(b, "_id", &id);
    bson_finish(b);
    mongo_remove(gfs->client, gfs->files_ns, b, NULL);
    bson_destroy(b);

    /* Remove all chunks from the file with the specified id */
    bson_init(b);
    bson_append_oid(b, "files_id", &id);
    bson_finish(b);
    ret = mongo_remove(gfs->client, gfs->chunks_ns, b, NULL);
    bson_destroy(b);
  }

  mongo_cursor_destroy(files);
  return ret;
}

MONGO_EXPORT int gridfs_find_query( gridfs *gfs, const bson *query, gridfile *gfile ) {

  bson uploadDate[1];
  bson finalQuery[1];
  bson out[1];
  int i;

  bson_init(uploadDate);
  bson_append_int(uploadDate, "uploadDate",  - 1);
  bson_finish(uploadDate);

  bson_init(finalQuery);
  bson_append_bson(finalQuery, "query", query);
  bson_append_bson(finalQuery, "orderby", uploadDate);
  bson_finish(finalQuery);

  i = (mongo_find_one(gfs->client, gfs->files_ns,  finalQuery, NULL, out) == MONGO_OK);
  bson_destroy(uploadDate);
  bson_destroy(finalQuery);
  if (!i) {
    return MONGO_ERROR;
  } else {
    gridfile_init(gfs, out, gfile);
    bson_destroy(out);
    return MONGO_OK;
  }
}

MONGO_EXPORT int gridfs_find_filename(gridfs *gfs, const char *filename, gridfile *gfile){
  bson query[1];
  int res;

  bson_init(query);
  bson_append_string_uppercase( query, "filename", filename, gfs->caseInsensitive );
  bson_finish(query);
  res = gridfs_find_query(gfs, query, gfile);
  bson_destroy(query);
  return res;
}

/* ---------------- */
/* gridfile methods */
/* ---------------- */

/* gridfile private methods forward declarations */
static int gridfile_flush_pendingchunk(gridfile *gfile);
static void gridfile_init_flags(gridfile *gfile);
static void gridfile_init_length(gridfile *gfile);
static void gridfile_init_chunkSize(gridfile *gfile);

/* gridfile constructors, destructors and memory management */

MONGO_EXPORT int gridfile_init( gridfs *gfs, const bson *meta, gridfile *gfile ) {
  gfile->gfs = gfs;
  gfile->pos = 0;
  gfile->pending_len = 0;
  gfile->pending_data = NULL;
  gfile->meta = bson_alloc();
  if (gfile->meta == NULL) {
    return MONGO_ERROR;
  } if( meta ) { 
    bson_copy(gfile->meta, meta);
  } else {
    bson_init_empty(gfile->meta);
  }
  gridfile_init_chunkSize( gfile );
  gridfile_init_length( gfile );
  gridfile_init_flags( gfile );
  return MONGO_OK;
}

MONGO_EXPORT int gridfile_writer_done(gridfile *gfile) {

  int response = MONGO_OK;

  if (gfile->pending_len) {
    /* write any remaining pending chunk data.
     * pending data will always take up less than one chunk */
    response = gridfile_flush_pendingchunk(gfile);    
  }
  if( gfile->pending_data ) {
    bson_free(gfile->pending_data);    
    gfile->pending_data = NULL;   
  }
  if( response == MONGO_OK ) {
    /* insert into files collection */
    response = gridfs_insert_file(gfile->gfs, gfile->remote_name, gfile->id, gfile->length, gfile->content_type, gfile->flags, gfile->chunkSize);
  }
  if( gfile->remote_name ) {
    bson_free(gfile->remote_name);
    gfile->remote_name = NULL;
  }
  if( gfile->content_type ) {
    bson_free(gfile->content_type);
    gfile->content_type = NULL;
  }
  return response;
}

static void gridfile_init_chunkSize(gridfile *gfile){
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, "chunkSize") != BSON_EOO)
        if (bson_iterator_type(it) == BSON_INT)
            gfile->chunkSize = bson_iterator_int(it);
        else
            gfile->chunkSize = (int)bson_iterator_long(it);
    else
        gfile->chunkSize = DEFAULT_CHUNK_SIZE;
}

static void gridfile_init_length(gridfile *gfile) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, "length") != BSON_EOO)
        if (bson_iterator_type(it) == BSON_INT)
            gfile->length = (gridfs_offset)bson_iterator_int(it);
        else
            gfile->length = (gridfs_offset)bson_iterator_long(it);
    else
        gfile->length = 0;
}

static void gridfile_init_flags(gridfile *gfile) {
  bson_iterator it[1];

  if( bson_find(it, gfile->meta, "flags") != BSON_EOO )
    gfile->flags = bson_iterator_int(it);
  else
    gfile->flags = 0;
}

MONGO_EXPORT int gridfile_writer_init(gridfile *gfile, gridfs *gfs, const char *remote_name, const char *content_type, int flags ) {
  gridfile tmpFile;

  gfile->gfs = gfs;
  if (gridfs_find_filename(gfs, remote_name, &tmpFile) == MONGO_OK) {
    if( gridfile_exists(&tmpFile) ) {
      /* If file exists, then let's initialize members dedicated to coordinate writing operations 
       with existing file metadata */
      gfile->id = gridfile_get_id( &tmpFile );
      gridfile_init_length( &tmpFile );            
      gfile->length = tmpFile.length;  
      gfile->chunkSize = gridfile_get_chunksize( gfile );
      if( flags != GRIDFILE_DEFAULT) {
        gfile->flags = flags;
      } else {
        gridfile_init_flags( &tmpFile );
        gfile->flags = tmpFile.flags;
      }
    }
    gridfile_destroy( &tmpFile );
  } else {
    /* File doesn't exist, let's create a new bson id and initialize length to zero */
    bson_oid_gen(&(gfile->id));
    gfile->length = 0;
    /* File doesn't exist, lets use the flags passed as a parameter to this procedure call */
    gfile->flags = flags;
  }  

  /* We initialize chunk_num with zero, but it will get always calculated when calling 
     gridfile_load_pending_data_with_pos_chunk() or when calling gridfile_write_buffer() */
  gfile->chunk_num = 0; 
  gfile->pos = 0;

  gfile->remote_name = (char*)bson_malloc((int)strlen(remote_name) + 1);
  strcpy((char*)gfile->remote_name, remote_name);

  gfile->content_type = (char*)bson_malloc((int)strlen(content_type) + 1);
  strcpy((char*)gfile->content_type, content_type);  

  gfile->pending_len = 0;
  /* Let's pre-allocate DEFAULT_CHUNK_SIZE bytes into pending_data then we don't need to worry 
     about doing realloc everywhere we want use the pending_data buffer */
  gfile->pending_data = (char*) bson_malloc((int)gridfs_pending_data_size(gfile->flags));

  return MONGO_OK;
}

MONGO_EXPORT void gridfile_destroy(gridfile *gfile) {
  if( gfile->meta ) { 
    bson_destroy(gfile->meta);
    bson_dealloc(gfile->meta);
    gfile->meta = NULL;
  }  
}

/* gridfile accessors */

MONGO_EXPORT bson_oid_t gridfile_get_id( const gridfile *gfile ) {
  bson_iterator it[1];

    if (bson_find(it, gfile->meta, "_id") != BSON_EOO)
        if (bson_iterator_type(it) == BSON_OID)
            return *bson_iterator_oid(it);
        else
            return gfile->id;
    else
        return gfile->id;
}

MONGO_EXPORT bson_bool_t gridfile_exists( const gridfile *gfile ) {
  /* File exists if gfile and gfile->meta BOTH are != NULL */
  return (bson_bool_t)(gfile != NULL && gfile->meta != NULL);
}

MONGO_EXPORT const char *gridfile_get_filename( const gridfile *gfile ) {
    bson_iterator it[1];

    if (gfile->gfs->caseInsensitive && bson_find( it, gfile->meta, "realFilename" ) != BSON_EOO)
        return bson_iterator_string(it); 
    if (bson_find(it, gfile->meta, "filename") != BSON_EOO)
        return bson_iterator_string(it);
    else
        return gfile->remote_name;
}

MONGO_EXPORT int gridfile_get_chunksize( const gridfile *gfile ) {
    bson_iterator it[1];

    if (gfile->chunkSize)
        return gfile->chunkSize;
    else if (bson_find(it, gfile->meta, "chunkSize") != BSON_EOO)
        return bson_iterator_int(it);
    else
        return DEFAULT_CHUNK_SIZE;
}

MONGO_EXPORT gridfs_offset gridfile_get_contentlength( const gridfile *gfile ) {
  gridfs_offset estimatedLen;
  estimatedLen = gfile->pending_len ? gfile->chunk_num * gridfile_get_chunksize( gfile ) + gfile->pending_len : gfile->length;
  return MAX( estimatedLen, gfile->length );
}

MONGO_EXPORT const char *gridfile_get_contenttype( const gridfile *gfile ) {
    bson_iterator it[1];

    if ( bson_find(it, gfile->meta, "contentType") != BSON_EOO )
        return bson_iterator_string(it);
    else
        return NULL;
}

MONGO_EXPORT bson_date_t gridfile_get_uploaddate( const gridfile *gfile ) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, "uploadDate") != BSON_EOO)
        return bson_iterator_date(it);
    else
        return 0;
}

MONGO_EXPORT const char *gridfile_get_md5( const gridfile *gfile ) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, "md5") != BSON_EOO )
        return bson_iterator_string(it);
    else
        return NULL;
}

MONGO_EXPORT void gridfile_set_flags(gridfile *gfile, int flags) {
    gfile->flags = flags;
}

MONGO_EXPORT int gridfile_get_flags( const gridfile *gfile ) {
  return gfile->flags;
}

MONGO_EXPORT const char *gridfile_get_field(gridfile *gfile, const char *name) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, name) != BSON_EOO)
        return bson_iterator_value(it);
    else
        return NULL;
}

MONGO_EXPORT bson_bool_t gridfile_get_boolean( const gridfile *gfile, const char *name ) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, name) != BSON_EOO)
        return bson_iterator_bool(it);
    else
        return 0;
}

MONGO_EXPORT void gridfile_get_metadata( const gridfile *gfile, bson *out, bson_bool_t copyData ) {
    bson_iterator it[1];

    if (bson_find(it, gfile->meta, "metadata") != BSON_EOO)
        bson_iterator_subobject_init(it, out, copyData);
    else
        bson_init_empty(out);
}

/* ++++++++++++++++++++++++++++++++ */
/* gridfile data management methods */
/* ++++++++++++++++++++++++++++++++ */

MONGO_EXPORT int gridfile_get_numchunks( const gridfile *gfile ) {
    bson_iterator it[1];
    gridfs_offset length;
    gridfs_offset chunkSize;
    double numchunks;

    if (bson_find(it, gfile->meta, "length") != BSON_EOO)
        if (bson_iterator_type(it) == BSON_INT)
            length = (gridfs_offset)bson_iterator_int(it);
        else
            length = (gridfs_offset)bson_iterator_long(it);
    else
        length = 0;

    if (bson_find(it, gfile->meta, "chunkSize") != BSON_EOO)
        if (bson_iterator_type(it) == BSON_INT)
            chunkSize = bson_iterator_int(it);
        else
            chunkSize = (int)bson_iterator_long(it);
    else
        chunkSize = DEFAULT_CHUNK_SIZE;

    numchunks = ((double)length / (double)chunkSize);
    return (numchunks - (int)numchunks > 0) ? (int)(numchunks + 1): (int)(numchunks);
}

static void gridfile_prepare_chunk_key_bson(bson *q, bson_oid_t *id, int chunk_num) {
  bson_init(q);
  bson_append_int(q, "n", chunk_num);
  bson_append_oid(q, "files_id", id);
  bson_finish(q);
}

static int gridfile_flush_pendingchunk(gridfile *gfile) {
    bson *oChunk;
    bson q[1];
    char* targetBuf = NULL;
    int res = MONGO_OK;

    if (gfile->pending_len) {
        size_t finish_position_after_flush;
        oChunk = chunk_new( gfile->id, gfile->chunk_num, &targetBuf, gfile->pending_data, gfile->pending_len, gfile->flags );
        gridfile_prepare_chunk_key_bson( q, &gfile->id, gfile->chunk_num );    
        res = mongo_update(gfile->gfs->client, gfile->gfs->chunks_ns, q, oChunk, MONGO_UPDATE_UPSERT, NULL);
        bson_destroy(q);
        chunk_free(oChunk);    
        if( res == MONGO_OK ){      
            finish_position_after_flush = (gfile->chunk_num * gfile->chunkSize) + gfile->pending_len;
            if (finish_position_after_flush > gfile->length)
                gfile->length = finish_position_after_flush;
            gfile->chunk_num++;
            gfile->pending_len = 0;
        }
    }
    if (targetBuf && targetBuf != gfile->pending_data)
        bson_free( targetBuf );
    return res;
}

static int gridfile_load_pending_data_with_pos_chunk(gridfile *gfile) {
  int chunk_len;
  const char *chunk_data;
  bson_iterator it[1];
  bson chk;
  char* targetBuffer = NULL;
  size_t targetBufferLen = 0;

  chk.dataSize = 0;
  gridfile_get_chunk(gfile, (int)(gfile->pos / DEFAULT_CHUNK_SIZE), &chk);
  if (chk.dataSize <= 5) {
        if( chk.data ) {
            bson_destroy( &chk );
        }
        return MONGO_ERROR;
  }
  if( bson_find(it, &chk, "data") != BSON_EOO){
    chunk_len = bson_iterator_bin_len(it);
    chunk_data = bson_iterator_bin_data(it);
    gridfs_read_filter( &targetBuffer, &targetBufferLen, chunk_data, (size_t)chunk_len, gfile->flags );
    gfile->pending_len = (int)targetBufferLen;
    gfile->chunk_num = (int)(gfile->pos / DEFAULT_CHUNK_SIZE);
    if( targetBufferLen ) {
      memcpy(gfile->pending_data, targetBuffer, targetBufferLen);
    }
  } else {
    bson_destroy( &chk );
    return MONGO_ERROR;
  }
  bson_destroy( &chk );
  if( targetBuffer && targetBuffer != chunk_data )
    bson_free( targetBuffer );
  return MONGO_OK;
}

MONGO_EXPORT gridfs_offset gridfile_write_buffer(gridfile *gfile, const char *data, gridfs_offset length) {

  bson *oChunk;
  bson q[1];
  size_t buf_pos, buf_bytes_to_write;    
  gridfs_offset bytes_left = length;
  char* targetBuf = NULL;
  int memAllocated = 0;

  gfile->chunk_num = (int)(gfile->pos / DEFAULT_CHUNK_SIZE);
  buf_pos = (int)(gfile->pos - (gfile->pos / DEFAULT_CHUNK_SIZE) * DEFAULT_CHUNK_SIZE);
  /* First let's see if our current position is an an offset > 0 from the beginning of the current chunk. 
     If so, then we need to preload current chunk and merge the data into it using the pending_data field
     of the gridfile gfile object. We will flush the data if we fill in the chunk */
  if( buf_pos ) {
    if( !gfile->pending_len && gridfile_load_pending_data_with_pos_chunk( gfile ) != MONGO_OK ) return 0;           
    buf_bytes_to_write = (size_t)MIN( length, DEFAULT_CHUNK_SIZE - buf_pos );
    memcpy( &gfile->pending_data[buf_pos], data, buf_bytes_to_write);
    if ( buf_bytes_to_write + buf_pos > gfile->pending_len ) {
      gfile->pending_len = buf_bytes_to_write + buf_pos;
    }
    gfile->pos += buf_bytes_to_write;
    if( buf_bytes_to_write + buf_pos >= DEFAULT_CHUNK_SIZE && gridfile_flush_pendingchunk(gfile) != MONGO_OK ) return 0;
    bytes_left -= buf_bytes_to_write;
    data += buf_bytes_to_write;
  }

  /* If there's still more data to be written and they happen to be full chunks, we will loop thru and 
     write all full chunks without the need for preloading the existing chunk */
  while( bytes_left >= DEFAULT_CHUNK_SIZE ) {
    int res; 
    if( (oChunk = chunk_new( gfile->id, gfile->chunk_num, &targetBuf, data, DEFAULT_CHUNK_SIZE, gfile->flags )) == NULL) return length - bytes_left;
    memAllocated = targetBuf != data;
    gridfile_prepare_chunk_key_bson(q, &gfile->id, gfile->chunk_num);
    res = mongo_update(gfile->gfs->client, gfile->gfs->chunks_ns, q, oChunk, MONGO_UPDATE_UPSERT, NULL);
    bson_destroy(q );
    chunk_free(oChunk);
    if( res != MONGO_OK ) return length - bytes_left;
    bytes_left -= DEFAULT_CHUNK_SIZE;
    gfile->chunk_num++;
    gfile->pos += DEFAULT_CHUNK_SIZE;
    if (gfile->pos > gfile->length) {
      gfile->length = gfile->pos;
    }
    data += DEFAULT_CHUNK_SIZE;
  }  

  /* Finally, if there's still remaining bytes left to write, we will preload the current chunk and merge the 
     remaining bytes into pending_data buffer */
  if ( bytes_left > 0 ) {
    /* Let's preload the chunk we are writing IF the current chunk is not already in memory
       AND if after writing the remaining buffer there's should be trailing data that we don't
       want to loose */ 
    if( !gfile->pending_len && gfile->pos + bytes_left < gfile->length && gridfile_load_pending_data_with_pos_chunk( gfile ) != MONGO_OK ) 
      return length - bytes_left;
    memcpy( gfile->pending_data, data, (size_t) bytes_left );
    if(  bytes_left > gfile->pending_len )
      gfile->pending_len = (int) bytes_left;
    gfile->pos += bytes_left;  
  }

  if( memAllocated ){
    bson_free( targetBuf );
  }
  return length;
}

MONGO_EXPORT void gridfile_get_chunk(gridfile *gfile, int n, bson *out) {
  bson query[1];

  bson_oid_t id;
  int result;

  bson_init(query);  
  id = gridfile_get_id( gfile );
  bson_append_oid(query, "files_id", &id);
  bson_append_int(query, "n", n);
  bson_finish(query);

  result = (mongo_find_one(gfile->gfs->client, gfile->gfs->chunks_ns, query, NULL, out) == MONGO_OK);
  bson_destroy(query);
  if (!result)
    bson_copy(out, bson_shared_empty());
}

MONGO_EXPORT mongo_cursor *gridfile_get_chunks(gridfile *gfile, size_t start, size_t size) {
  bson_iterator it[1];
  bson_oid_t id;
  bson gte[1];
  bson query[1];
  bson orderby[1];
  bson command[1];
  mongo_cursor *cursor;

  if( bson_find(it, gfile->meta, "_id") != BSON_EOO)
    id =  *bson_iterator_oid(it);
  else
    id = gfile->id;

  bson_init(query);
  bson_append_oid(query, "files_id", &id);
  if (size == 1) {
    bson_append_int(query, "n", (int)start);
  } else {
    bson_init(gte);
    bson_append_int(gte, "$gte", (int)start);
    bson_finish(gte);
    bson_append_bson(query, "n", gte);
    bson_destroy(gte);
  }
  bson_finish(query);

  bson_init(orderby);
  bson_append_int(orderby, "n", 1);
  bson_finish(orderby);

  bson_init(command);
  bson_append_bson(command, "query", query);
  bson_append_bson(command, "orderby", orderby);
  bson_finish(command);

  cursor = mongo_find(gfile->gfs->client, gfile->gfs->chunks_ns,  command, NULL, (int)size, 0, 0);

  bson_destroy(command);
  bson_destroy(query);
  bson_destroy(orderby);

  return cursor;
}

static gridfs_offset gridfile_read_from_pending_buffer(gridfile *gfile, gridfs_offset totalBytesToRead, char* buf, int *first_chunk);
static gridfs_offset gridfile_load_from_chunks(gridfile *gfile, int total_chunks, gridfs_offset chunksize, mongo_cursor *chunks, char* buf, 
                                               gridfs_offset bytes_left);

MONGO_EXPORT gridfs_offset gridfile_read_buffer( gridfile *gfile, char *buf, gridfs_offset size ) {
  mongo_cursor *chunks;  

  int first_chunk;  
  int total_chunks;
  gridfs_offset chunksize;
  gridfs_offset contentlength;
  gridfs_offset bytes_left;
  gridfs_offset realSize = 0;

  contentlength = gridfile_get_contentlength(gfile);
  chunksize = gridfile_get_chunksize(gfile);
  size = MIN( contentlength - gfile->pos, size );
  bytes_left = size;

  first_chunk = (int)((gfile->pos) / chunksize);  
  total_chunks = (int)((gfile->pos + size - 1) / chunksize) - first_chunk + 1;
  
  if( (realSize = gridfile_read_from_pending_buffer( gfile, bytes_left, buf, &first_chunk )) > 0 ) {
    gfile->pos += realSize;    
    if( --total_chunks <= 0) {
      return realSize;
    }
    buf += realSize;
    bytes_left -= realSize;
    if( gridfile_flush_pendingchunk( gfile ) != MONGO_OK ){
      /* Let's abort the read operation here because we could not flush the buffer */
      return realSize; 
    }
  }; 

  chunks = gridfile_get_chunks(gfile, first_chunk, total_chunks);
  realSize += gridfile_load_from_chunks( gfile, total_chunks, chunksize, chunks, buf, bytes_left);  
  mongo_cursor_destroy(chunks);

  gfile->pos += realSize;

  return realSize;
}

static gridfs_offset gridfile_read_from_pending_buffer(gridfile *gfile, gridfs_offset totalBytesToRead, char* buf, 
                                                       int *first_chunk){
  gridfs_offset realSize = 0;
  if( gfile->pending_len > 0 && *first_chunk == gfile->chunk_num) {    
    char *chunk_data;
    gridfs_offset chunksize = gridfile_get_chunksize(gfile);
    gridfs_offset ofs = gfile->pos - gfile->chunk_num * chunksize;
    realSize = MIN( totalBytesToRead, gfile->pending_len - ofs );
    chunk_data = gfile->pending_data + ofs;
    memcpy( buf, chunk_data, (size_t)realSize );                
    (*first_chunk)++; 
  }; 
  return realSize;     
}

static gridfs_offset gridfile_fill_buf_from_chunk(gridfile *gfile, const bson *chunk, gridfs_offset chunksize, char **buf, int *allocatedMem, char **targetBuf, 
                                                  size_t *targetBufLen, gridfs_offset *bytes_left, int chunkNo);

static gridfs_offset gridfile_load_from_chunks(gridfile *gfile, int total_chunks, gridfs_offset chunksize, mongo_cursor *chunks, char* buf, 
                                               gridfs_offset bytes_left){
  int i;
  char* targetBuf = NULL; 
  size_t targetBufLen = 0;
  int allocatedMem = 0;
  gridfs_offset realSize = 0;
  
  for (i = 0; i < total_chunks; i++) {
    if( mongo_cursor_next(chunks) != MONGO_OK ){
      break;
    }
    realSize += gridfile_fill_buf_from_chunk( gfile, &chunks->current, chunksize, &buf, &allocatedMem, &targetBuf, &targetBufLen, &bytes_left, i); 
  }
  if( allocatedMem ) {
    bson_free( targetBuf );
  }
  return realSize;
}

static gridfs_offset gridfile_fill_buf_from_chunk(gridfile *gfile, const bson *chunk, gridfs_offset chunksize, char **buf, int *allocatedMem, char **targetBuf, 
                                                  size_t *targetBufLen, gridfs_offset *bytes_left, int chunkNo){
  bson_iterator it[1];
  gridfs_offset chunk_len;
  const char *chunk_data;

  if( bson_find(it, chunk, "data") != BSON_EOO ) {
    chunk_len = bson_iterator_bin_len(it);
    chunk_data = bson_iterator_bin_data(it);  
    if( gridfs_read_filter( targetBuf, targetBufLen, chunk_data, (size_t)chunk_len, gfile->flags ) != 0) return 0;
    *allocatedMem = *targetBuf != chunk_data;
    chunk_data = *targetBuf;
    if (chunkNo == 0) {      
      chunk_data += (gfile->pos) % chunksize;
      *targetBufLen -= (size_t)( (gfile->pos) % chunksize );
    } 
    if (*bytes_left > *targetBufLen) {
      memcpy(*buf, chunk_data, *targetBufLen);
      *bytes_left -= *targetBufLen; 
      *buf += *targetBufLen;
      return *targetBufLen;
    } else {
      memcpy(*buf, chunk_data, (size_t)(*bytes_left));
      return *bytes_left;
    }
  } else {
    bson_fatal_msg( 0, "Chunk object doesn't have 'data' attribute" );
    return 0;
  }
}

MONGO_EXPORT gridfs_offset gridfile_seek(gridfile *gfile, gridfs_offset offset) {
  gridfs_offset length;
  gridfs_offset chunkSize;
  gridfs_offset newPos;

  chunkSize = gridfile_get_chunksize( gfile );
  length = gridfile_get_contentlength( gfile );
  newPos = MIN( length, offset );

  /* If we are seeking to the next chunk or prior to the current chunks let's flush the pending chunk */
  if (gfile->pending_len && (newPos >= (gfile->chunk_num + 1) * chunkSize || newPos < gfile->chunk_num * chunkSize) &&
    gridfile_flush_pendingchunk( gfile ) != MONGO_OK ) return gfile->pos;  
  gfile->pos = newPos;
  return newPos;
}

MONGO_EXPORT gridfs_offset gridfile_write_file(gridfile *gfile, FILE *stream) {
  char buffer[DEFAULT_CHUNK_SIZE];
  size_t data_read, data_written = 0;  
  gridfs_offset total_written = 0;

  do {
    data_read = (size_t)gridfile_read_buffer( gfile, buffer, DEFAULT_CHUNK_SIZE );
    if( data_read > 0 ){
      data_written = fwrite( buffer, sizeof(char), data_read, stream );
      total_written += data_written;              
    }    
  } while(( data_read > 0 ) && ( data_written == data_read ));

  return total_written;
}

static int gridfile_remove_chunks( gridfile *gfile, int deleteFromChunk){
  bson q[1];
  bson_oid_t id = gridfile_get_id( gfile );
  int res;

  bson_init( q );
  bson_append_oid(q, "files_id", &id);
  if( deleteFromChunk >= 0 ) {
    bson_append_start_object( q, "n" );
    bson_append_int( q, "$gte", deleteFromChunk );
    bson_append_finish_object( q );
  }
  bson_finish( q );
  res = mongo_remove( gfile->gfs->client, gfile->gfs->chunks_ns, q, NULL);
  bson_destroy( q );
  return res;
}

MONGO_EXPORT gridfs_offset gridfile_truncate(gridfile *gfile, gridfs_offset newSize) {

  int deleteFromChunk;

  if ( newSize > gridfile_get_contentlength( gfile ) ) {
    return gridfile_seek( gfile, gridfile_get_contentlength( gfile ) );    
  }
  if( newSize > 0 ) {
    deleteFromChunk = (int)(newSize / gridfile_get_chunksize( gfile )); 
    if( gridfile_seek(gfile, newSize) != newSize ) return gfile->length;
    if( gfile->pos % gridfile_get_chunksize( gfile ) ) {
      if( !gfile->pending_len && gridfile_load_pending_data_with_pos_chunk( gfile ) != MONGO_OK ) return gfile->length;
      gfile->pending_len = gfile->pos % gridfile_get_chunksize( gfile ); /* This will truncate the preloaded chunk */
      if( gridfile_flush_pendingchunk( gfile ) != MONGO_OK ) return gfile->length;
      deleteFromChunk++;
    }
    /* Now let's remove the trailing chunks resulting from truncation */
    if( gridfile_remove_chunks( gfile, deleteFromChunk ) != MONGO_OK ) return gfile->length;
    gfile->length = newSize;
  } else {
    /* Expected file size is zero. We will remove ALL chunks */
    if( gridfile_remove_chunks( gfile, -1 ) != MONGO_OK) return gfile->length;    
    gfile->length = 0;
    gfile->pos = 0;
  }
  return gfile->length;
}

MONGO_EXPORT gridfs_offset gridfile_expand(gridfile *gfile, gridfs_offset bytesToExpand) {
  gridfs_offset fileSize, newSize, curPos, toWrite, bufSize;  

  char* buf;

  fileSize = gridfile_get_contentlength( gfile );
  newSize = fileSize + bytesToExpand;
  curPos = fileSize;
  bufSize = gridfile_get_chunksize ( gfile );
  buf = (char*)bson_malloc( (size_t)bufSize );
  
  memset( buf, 0, (size_t)bufSize );
  gridfile_seek( gfile, fileSize );

  while( curPos < newSize ) {
    toWrite = bufSize - curPos % bufSize;
    if( toWrite + curPos > newSize ) {
      toWrite = newSize - curPos;
    }
    /* If driver doesn't write all data request, we will cancel expansion and return how far we got... */
    if( gridfile_write_buffer( gfile, (const char*)buf, toWrite ) != toWrite) return curPos;
    curPos += toWrite;
  }

  bson_free( buf );
  return newSize;
}

MONGO_EXPORT gridfs_offset gridfile_set_size(gridfile *gfile, gridfs_offset newSize) {
  gridfs_offset fileSize;

  fileSize = gridfile_get_contentlength( gfile );
  if( newSize <= fileSize ) {
    return gridfile_truncate( gfile, newSize );
  } else {            
    return gridfile_expand( gfile, newSize - fileSize );
  }
}

/*==============================================================*/
/* --- env.h */
#define INVALID_SOCKET (-1)

/*==============================================================*/
/* --- env.c */

#ifndef NI_MAXSERV
# define NI_MAXSERV 32
#endif

int mongo_env_close_socket( SOCKET socket ) {
    return close( socket );
}

static int mongo_env_sock_init( void ) {
    return 0;
}

static int mongo_env_write_socket( mongo *conn, const void *buf, size_t len ) {
    const char *cbuf = buf;
#ifdef __APPLE__
    int flags = 0;
#else
    int flags = MSG_NOSIGNAL;
#endif

    while ( len ) {
        ssize_t sent = send( conn->sock, cbuf, len, flags );
        if ( sent == -1 ) {
            if (errno == EPIPE)
                conn->connected = 0;
            __mongo_set_error( conn, MONGO_IO_ERROR, strerror( errno ), errno );
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }

    return MONGO_OK;
}

static int mongo_env_read_socket( mongo *conn, void *buf, size_t len ) {
    char *cbuf = buf;
    while ( len ) {
        ssize_t sent = recv( conn->sock, cbuf, len, 0 );
        if ( sent == 0 || sent == -1 ) {
            __mongo_set_error( conn, MONGO_IO_ERROR, strerror( errno ), errno );
            return MONGO_ERROR;
        }
        cbuf += sent;
        len -= sent;
    }

    return MONGO_OK;
}

static int mongo_env_set_socket_op_timeout( mongo *conn, int millis ) {
    struct timeval tv;
    tv.tv_sec = millis / 1000;
    tv.tv_usec = ( millis % 1000 ) * 1000;

    if ( setsockopt( conn->sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof( tv ) ) == -1 ) {
        conn->err = MONGO_IO_ERROR;
        __mongo_set_error( conn, MONGO_IO_ERROR, "setsockopt SO_RCVTIMEO failed.", errno );
        return MONGO_ERROR;
    }

    if ( setsockopt( conn->sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof( tv ) ) == -1 ) {
        __mongo_set_error( conn, MONGO_IO_ERROR, "setsockopt SO_SNDTIMEO failed.", errno );
        return MONGO_ERROR;
    }

    return MONGO_OK;
}

static int mongo_env_unix_socket_connect( mongo *conn, const char *sock_path ) {
    struct sockaddr_un addr = {};
    int status, len;

    conn->connected = 0;

    conn->sock = socket( AF_UNIX, SOCK_STREAM, 0 );

    if ( conn->sock == INVALID_SOCKET ) {
        return MONGO_ERROR;
    }
    
    addr.sun_family = AF_UNIX;
    strncpy( addr.sun_path, sock_path, sizeof(addr.sun_path) - 1 );
    len = sizeof( addr );

    status = connect( conn->sock, (struct sockaddr *) &addr, len );
    if( status < 0 ) {
        mongo_env_close_socket( conn->sock );
        conn->sock = 0;
        conn->err = MONGO_CONN_FAIL;
        return MONGO_ERROR;
    }

    conn->connected = 1;

    return MONGO_OK;
}

static int mongo_env_socket_connect( mongo *conn, const char *host, int port ) {
    char port_str[NI_MAXSERV];
    int status;

    struct addrinfo ai_hints;
    struct addrinfo *ai_list = NULL;
    struct addrinfo *ai_ptr = NULL;

    if ( port < 0 ) {
        return mongo_env_unix_socket_connect( conn, host );
    }

    conn->sock = 0;
    conn->connected = 0;
    sprintf(port_str,"%d",port);

    bson_sprintf( port_str, "%d", port );

    memset( &ai_hints, 0, sizeof( ai_hints ) );
#ifdef AI_ADDRCONFIG
    ai_hints.ai_flags = AI_ADDRCONFIG;
#endif
    ai_hints.ai_family = AF_UNSPEC;
    ai_hints.ai_socktype = SOCK_STREAM;

    status = getaddrinfo( host, port_str, &ai_hints, &ai_list );
    if ( status != 0 ) {
        bson_errprintf( "getaddrinfo failed: %s", gai_strerror( status ) );
        conn->err = MONGO_CONN_ADDR_FAIL;
        return MONGO_ERROR;
    }

    for ( ai_ptr = ai_list; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next ) {
        conn->sock = socket( ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol );
        if ( conn->sock == INVALID_SOCKET ) {
            continue;
        }
        
        status = connect( conn->sock, ai_ptr->ai_addr, ai_ptr->ai_addrlen );
        if ( status != 0 ) {
            mongo_env_close_socket( conn->sock );
            conn->sock = 0;
            continue;
        }
#if __APPLE__
        {
            int flag = 1;
            if ( setsockopt( conn->sock, SOL_SOCKET, SO_NOSIGPIPE,
                             ( void * ) &flag, sizeof( flag ) ) == -1 ) {
                conn->err = MONGO_IO_ERROR;
                __mongo_set_error( conn, MONGO_IO_ERROR, "setsockopt SO_NOSIGPIPE failed.", errno );
                return MONGO_ERROR;
            }
        }
#endif

        if ( ai_ptr->ai_protocol == IPPROTO_TCP ) {
            int flag = 1;
            if ( setsockopt( conn->sock, IPPROTO_TCP, TCP_NODELAY,
                             ( void * ) &flag, sizeof( flag ) ) == -1 ) {
                conn->err = MONGO_IO_ERROR;
                __mongo_set_error( conn, MONGO_IO_ERROR, "setsockopt SO_NOSIGPIPE failed.", errno );
                return MONGO_ERROR;
            }
            if ( conn->op_timeout_ms > 0 )
                mongo_env_set_socket_op_timeout( conn, conn->op_timeout_ms );
        }

        conn->connected = 1;
        break;
    }

    freeaddrinfo( ai_list );

    if ( ! conn->connected ) {
        conn->err = MONGO_CONN_FAIL;
        return MONGO_ERROR;
    }

    return MONGO_OK;
}

/*==============================================================*/
/* --- mongo.c */

MONGO_EXPORT mongo* mongo_alloc( void ) {
    return ( mongo* )bson_malloc( sizeof( mongo ) );
}


MONGO_EXPORT void mongo_dealloc(mongo* conn) {
    bson_free( conn );
}

MONGO_EXPORT int mongo_get_err(mongo* conn) {
    return conn->err;
}


MONGO_EXPORT int mongo_is_connected(mongo* conn) {
    return conn->connected != 0;
}


MONGO_EXPORT int mongo_get_op_timeout(mongo* conn) {
    return conn->op_timeout_ms;
}


static const char* _get_host_port(mongo_host_port* hp) {
    static char _hp[sizeof(hp->host)+12];
    bson_sprintf(_hp, "%s:%d", hp->host, hp->port);
    return _hp;
}


MONGO_EXPORT const char* mongo_get_primary(mongo* conn) {
    mongo* conn_ = (mongo*)conn;
    if( !(conn_->connected) || (conn_->primary->host[0] == '\0') )
        return NULL;
    return _get_host_port(conn_->primary);
}


MONGO_EXPORT SOCKET mongo_get_socket(mongo* conn) {
    mongo* conn_ = (mongo*)conn;
    return conn_->sock;
}


MONGO_EXPORT int mongo_get_host_count(mongo* conn) {
    mongo_replica_set* r = conn->replica_set;
    mongo_host_port* hp;
    int count = 0;
    if (!r) return 0;
    for (hp = r->hosts; hp; hp = hp->next)
        ++count;
    return count;
}


MONGO_EXPORT const char* mongo_get_host(mongo* conn, int i) {
    mongo_replica_set* r = conn->replica_set;
    mongo_host_port* hp;
    int count = 0;
    if (!r) return 0;
    for (hp = r->hosts; hp; hp = hp->next) {
        if (count == i)
            return _get_host_port(hp);
        ++count;
    }
    return 0;
}

MONGO_EXPORT mongo_write_concern* mongo_write_concern_alloc( void ) {
    return ( mongo_write_concern* )bson_malloc( sizeof( mongo_write_concern ) );
}


MONGO_EXPORT void mongo_write_concern_dealloc( mongo_write_concern* write_concern ) {
    bson_free( write_concern );
}


MONGO_EXPORT mongo_cursor* mongo_cursor_alloc( void ) {
    return ( mongo_cursor* )bson_malloc( sizeof( mongo_cursor ) );
}


MONGO_EXPORT void mongo_cursor_dealloc( mongo_cursor* cursor ) {
    bson_free( cursor );
}


MONGO_EXPORT int  mongo_get_server_err(mongo* conn) {
    return conn->lasterrcode;
}


MONGO_EXPORT const char*  mongo_get_server_err_string(mongo* conn) {
    return conn->lasterrstr;
}

MONGO_EXPORT void __mongo_set_error( mongo *conn, mongo_error_t err, const char *str,
                                     int errcode ) {
    size_t str_size = 1;
    conn->err = err;
    conn->errcode = errcode;
    if( str ) {
        str_size = strlen( str ) + 1;
        if (str_size > MONGO_ERR_LEN) str_size = MONGO_ERR_LEN;
        memcpy( conn->errstr, str, str_size );
    }
    conn->errstr[str_size-1] = '\0';
}

MONGO_EXPORT void mongo_clear_errors( mongo *conn ) {
    conn->err = MONGO_CONN_SUCCESS;
    conn->errcode = 0;
    conn->lasterrcode = 0;
    conn->errstr[0] = 0;
    conn->lasterrstr[0] = 0;
}

/* Note: this function returns a char* which must be freed. */
static char *mongo_ns_to_cmd_db( const char *ns ) {
    char *current = NULL;
    char *cmd_db_name = NULL;
    int len = 0;

    for( current = (char *)ns; *current != '.'; current++ ) {
        len++;
    }

    cmd_db_name = (char *)bson_malloc( len + 6 );
    strncpy( cmd_db_name, ns, len );
    strncpy( cmd_db_name + len, ".$cmd", 6 );

    return cmd_db_name;
}

MONGO_EXPORT int mongo_validate_ns( mongo *conn, const char *ns ) {
    char *last = NULL;
    char *current = NULL;
    const char *db_name = ns;
    char *collection_name = NULL;
    char errmsg[64];
    int ns_len = 0;

    /* If the first character is a '.', fail. */
    if( *ns == '.' ) {
        __mongo_set_error( conn, MONGO_NS_INVALID, "ns cannot start with a '.'.", 0 );
        return MONGO_ERROR;
    }

    /* Find the division between database and collection names. */
    for( current = (char *)ns; *current != '\0'; current++ ) {
        if( *current == '.' ) {
            current++;
            break;
        }
    }

    /* Fail if the collection part starts with a dot. */
    if( *current == '.' ) {
        __mongo_set_error( conn, MONGO_NS_INVALID, "ns cannot start with a '.'.", 0 );
        return MONGO_ERROR;
    }

    /* Fail if collection length is 0.
     * or the ns doesn't contain a '.'. */
    if( *current == '\0' ) {
        __mongo_set_error( conn, MONGO_NS_INVALID, "Collection name missing.", 0 );
        return MONGO_ERROR;
    }


    /* Point to the beginning of the collection name. */
    collection_name = current;

    /* Ensure that the database name is greater than one char.*/
    if( collection_name - 1 == db_name ) {
        __mongo_set_error( conn, MONGO_NS_INVALID, "Database name missing.", 0 );
        return MONGO_ERROR;
    }

    /* Go back and validate the database name. */
    for( current = (char *)db_name; *current != '.'; current++ ) {
        switch( *current ) {
        case ' ':
        case '$':
        case '/':
        case '\\':
            __mongo_set_error( conn, MONGO_NS_INVALID,
                               "Database name may not contain ' ', '$', '/', or '\\'", 0 );
            return MONGO_ERROR;
        default:
            break;
        }

        ns_len++;
    }

    /* Add one to the length for the '.' character. */
    ns_len++;

    /* Now validate the collection name. */
    for( current = collection_name; *current != '\0'; current++ ) {

        /* Cannot have two consecutive dots. */
        if( last && *last == '.' && *current == '.' ) {
            __mongo_set_error( conn, MONGO_NS_INVALID,
                               "Collection may not contain two consecutive '.'", 0 );
            return MONGO_ERROR;
        }

        /* Cannot contain a '$' */
        if( *current == '$' ) {
            __mongo_set_error( conn, MONGO_NS_INVALID,
                               "Collection may not contain '$'", 0 );
            return MONGO_ERROR;
        }

        last = current;
        ns_len++;
    }

    if( ns_len > 128 ) {
        bson_sprintf( errmsg, "Namespace too long; has %d but must <= 128.",
                      ns_len );
        __mongo_set_error( conn, MONGO_NS_INVALID, errmsg, 0 );
        return MONGO_ERROR;
    }

    /* Cannot end with a '.' */
    if( *(current - 1) == '.' ) {
        __mongo_set_error( conn, MONGO_NS_INVALID,
                           "Collection may not end with '.'", 0 );
        return MONGO_ERROR;
    }

    return MONGO_OK;
}

static void mongo_set_last_error( mongo *conn, bson_iterator *it, bson *obj ) {
    bson_iterator iter[1];
    int result_len = bson_iterator_string_len( it );
    const char *result_string = bson_iterator_string( it );
    int len = result_len < MONGO_ERR_LEN ? result_len : MONGO_ERR_LEN;
    memcpy( conn->lasterrstr, result_string, len );
    iter[0] = *it;  // no side effects on the passed iter
    if( bson_find( iter, obj, "code" ) != BSON_NULL )
        conn->lasterrcode = bson_iterator_int( iter );
}

static const int ZERO = 0;
static const int ONE = 1;
static mongo_message *mongo_message_create( size_t len , int id , int responseTo , int op ) {
    mongo_message *mm;

    if( len >= INT32_MAX) {
        return NULL;
    }
    mm = ( mongo_message * )bson_malloc( len );
    if ( !id )
        id = rand();

    /* native endian (converted on send) */
    mm->head.len = ( int )len;
    mm->head.id = id;
    mm->head.responseTo = responseTo;
    mm->head.op = op;

    return mm;
}

/* Always calls bson_free(mm) */
static int mongo_message_send( mongo *conn, mongo_message *mm ) {
    mongo_header head; /* little endian */
    int res;
    bson_little_endian32( &head.len, &mm->head.len );
    bson_little_endian32( &head.id, &mm->head.id );
    bson_little_endian32( &head.responseTo, &mm->head.responseTo );
    bson_little_endian32( &head.op, &mm->head.op );

    res = mongo_env_write_socket( conn, &head, sizeof( head ) );
    if( res != MONGO_OK ) {
        bson_free( mm );
        return res;
    }

    res = mongo_env_write_socket( conn, &mm->data, mm->head.len - sizeof( head ) );
    if( res != MONGO_OK ) {
        bson_free( mm );
        return res;
    }

    bson_free( mm );
    return MONGO_OK;
}

static int mongo_read_response( mongo *conn, mongo_reply **reply ) {
    mongo_header head; /* header from network */
    mongo_reply_fields fields; /* header from network */
    mongo_reply *out;  /* native endian */
    unsigned int len;
    int res;

    if ( ( res = mongo_env_read_socket( conn, &head, sizeof( head ) ) ) != MONGO_OK ||
         ( res = mongo_env_read_socket( conn, &fields, sizeof( fields ) ) ) != MONGO_OK ) {
        return res;
    }

    bson_little_endian32( &len, &head.len );

    if ( len < sizeof( head )+sizeof( fields ) || len > 64*1024*1024 )
        return MONGO_READ_SIZE_ERROR;  /* most likely corruption */

    /*
     * mongo_reply matches the wire for observed environments (MacOS, Linux, Windows VC), but
     * the following incorporates possible differences with type sizes and padding/packing
     *
     * assert( sizeof(mongo_reply) - sizeof(char) - 16 - 20 + len >= len );
     * printf( "sizeof(mongo_reply) - sizeof(char) - 16 - 20 = %ld\n", sizeof(mongo_reply) - sizeof(char) - 16 - 20 );
     */
    out = ( mongo_reply * )bson_malloc( sizeof(mongo_reply) - sizeof(char) + len - 16 - 20 );

    out->head.len = len;
    bson_little_endian32( &out->head.id, &head.id );
    bson_little_endian32( &out->head.responseTo, &head.responseTo );
    bson_little_endian32( &out->head.op, &head.op );

    bson_little_endian32( &out->fields.flag, &fields.flag );
    bson_little_endian64( &out->fields.cursorID, &fields.cursorID );
    bson_little_endian32( &out->fields.start, &fields.start );
    bson_little_endian32( &out->fields.num, &fields.num );

    res = mongo_env_read_socket( conn, &out->objs, len - 16 - 20 ); /* was len-sizeof( head )-sizeof( fields ) */
    if( res != MONGO_OK ) {
        bson_free( out );
        return res;
    }

    *reply = out;

    return MONGO_OK;
}


static char *mongo_data_append( char *start , const void *data , size_t len ) {
    memcpy( start , data , len );
    return start + len;
}

static char *mongo_data_append32( char *start , const void *data ) {
    bson_little_endian32( start , data );
    return start + 4;
}

static char *mongo_data_append64( char *start , const void *data ) {
    bson_little_endian64( start , data );
    return start + 8;
}

/* Connection API */

static int mongo_check_is_master( mongo *conn ) {
    bson out;
    bson_iterator it;
    bson_bool_t ismaster = 0;
    int max_bson_size = MONGO_DEFAULT_MAX_BSON_SIZE;

    if ( mongo_simple_int_command( conn, "admin", "ismaster", 1, &out ) != MONGO_OK )
        return MONGO_ERROR;

    if( bson_find( &it, &out, "ismaster" ) )
        ismaster = bson_iterator_bool( &it );
    if( bson_find( &it, &out, "maxBsonObjectSize" ) )
        max_bson_size = bson_iterator_int( &it );
    conn->max_bson_size = max_bson_size;

    bson_destroy( &out );

    if( ismaster )
        return MONGO_OK;
    else {
        conn->err = MONGO_CONN_NOT_MASTER;
        return MONGO_ERROR;
    }
}

MONGO_EXPORT void mongo_init_sockets( void ) {
    mongo_env_sock_init();
}

/* WC1 is completely static */
static char WC1_data[] = {23,0,0,0,16,103,101,116,108,97,115,116,101,114,114,111,114,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0};
static bson WC1_cmd = {
    WC1_data, WC1_data, 128, 1, 0
};
static mongo_write_concern WC1 = { 1, 0, 0, 0, 0, &WC1_cmd }; /* w = 1 */

MONGO_EXPORT void mongo_init( mongo *conn ) {
    memset( conn, 0, sizeof( mongo ) );
    conn->max_bson_size = MONGO_DEFAULT_MAX_BSON_SIZE;
    mongo_set_write_concern( conn, &WC1 );
}

MONGO_EXPORT int mongo_client( mongo *conn , const char *host, int port ) {
    mongo_init( conn );

    conn->primary = (mongo_host_port*)bson_malloc( sizeof( mongo_host_port ) );
    snprintf( conn->primary->host, MAXHOSTNAMELEN, "%s", host);
    conn->primary->port = port;
    conn->primary->next = NULL;

    if( mongo_env_socket_connect( conn, host, port ) != MONGO_OK )
        return MONGO_ERROR;

    return mongo_check_is_master( conn );
}

MONGO_EXPORT int mongo_connect( mongo *conn , const char *host, int port ) {
    int ret;
    bson_errprintf("WARNING: mongo_connect() is deprecated, please use mongo_client()\n");
    ret = mongo_client( conn, host, port );
    mongo_set_write_concern( conn, 0 );
    return ret;
}

MONGO_EXPORT void mongo_replica_set_init( mongo *conn, const char *name ) {
    mongo_init( conn );

    conn->replica_set = (mongo_replica_set*)bson_malloc( sizeof( mongo_replica_set ) );
    conn->replica_set->primary_connected = 0;
    conn->replica_set->seeds = NULL;
    conn->replica_set->hosts = NULL;
    conn->replica_set->name = ( char * )bson_malloc( strlen( name ) + 1 );
    memcpy( conn->replica_set->name, name, strlen( name ) + 1  );

    conn->primary = (mongo_host_port*)bson_malloc( sizeof( mongo_host_port ) );
    conn->primary->host[0] = '\0';
    conn->primary->next = NULL;
}

MONGO_EXPORT void mongo_replset_init( mongo *conn, const char *name ) {
    bson_errprintf("WARNING: mongo_replset_init() is deprecated, please use mongo_replica_set_init()\n");
    mongo_replica_set_init( conn, name );
}

static void mongo_replica_set_add_node( mongo_host_port **list, const char *host, int port ) {
    mongo_host_port *host_port = (mongo_host_port*)bson_malloc( sizeof( mongo_host_port ) );
    host_port->port = port;
    host_port->next = NULL;
    snprintf( host_port->host, MAXHOSTNAMELEN, "%s", host);

    if( *list == NULL )
        *list = host_port;
    else {
        mongo_host_port *p = *list;
        while( p->next != NULL )
            p = p->next;
        p->next = host_port;
    }
}

static void mongo_replica_set_free_list( mongo_host_port **list ) {
    mongo_host_port *node = *list;
    mongo_host_port *prev;

    while( node != NULL ) {
        prev = node;
        node = node->next;
        bson_free( prev );
    }

    *list = NULL;
}

MONGO_EXPORT void mongo_replica_set_add_seed( mongo *conn, const char *host, int port ) {
    mongo_replica_set_add_node( &conn->replica_set->seeds, host, port );
}

MONGO_EXPORT void mongo_replset_add_seed( mongo *conn, const char *host, int port ) {
    bson_errprintf("WARNING: mongo_replset_add_seed() is deprecated, please use mongo_replica_set_add_seed()\n");
    mongo_replica_set_add_node( &conn->replica_set->seeds, host, port );
}

void mongo_parse_host( const char *host_string, mongo_host_port *host_port ) {
    int len, idx, split;
    len = split = idx = 0;

    /* Split the host_port string at the ':' */
    while( 1 ) {
        if( *( host_string + len ) == '\0' )
            break;
        if( *( host_string + len ) == ':' )
            split = len;

        len++;
    }

    /* If 'split' is set, we know the that port exists;
     * Otherwise, we set the default port. */
    idx = split ? split : len;
    memcpy( host_port->host, host_string, idx );
    memcpy( host_port->host + idx, "\0", 1 );
    if( split )
        host_port->port = atoi( host_string + idx + 1 );
    else
        host_port->port = MONGO_DEFAULT_PORT;
}

static void mongo_replica_set_check_seed( mongo *conn ) {
    bson out[1];
    const char *data;
    bson_iterator it[1];
    bson_iterator it_sub[1];
    const char *host_string;
    mongo_host_port *host_port = NULL;

    if( mongo_simple_int_command( conn, "admin", "ismaster", 1, out ) == MONGO_OK ) {

        if( bson_find( it, out, "hosts" ) ) {
            data = bson_iterator_value( it );
            bson_iterator_from_buffer( it_sub, data );

            /* Iterate over host list, adding each host to the
             * connection's host list. */
            while( bson_iterator_next( it_sub ) ) {
                host_string = bson_iterator_string( it_sub );

                host_port = (mongo_host_port*)bson_malloc( sizeof( mongo_host_port ) );

                if( host_port ) {
                    mongo_parse_host( host_string, host_port );
                    mongo_replica_set_add_node( &conn->replica_set->hosts,
                                                host_port->host, host_port->port );

                    bson_free( host_port );
                    host_port = NULL;
                }
            }
        }
    }

    bson_destroy(out);
    mongo_env_close_socket( conn->sock );
    conn->sock = 0;
    conn->connected = 0;
}

/* Find out whether the current connected node is master, and
 * verify that the node's replica set name matched the provided name
 */
static int mongo_replica_set_check_host( mongo *conn ) {

    bson out[1];
    bson_iterator it[1];
    bson_bool_t ismaster = 0;
    const char *set_name;
    int max_bson_size = MONGO_DEFAULT_MAX_BSON_SIZE;

    if ( mongo_simple_int_command( conn, "admin", "ismaster", 1, out ) == MONGO_OK ) {
        if( bson_find( it, out, "ismaster" ) )
            ismaster = bson_iterator_bool( it );

        if( bson_find( it, out, "maxBsonObjectSize" ) )
            max_bson_size = bson_iterator_int( it );
        conn->max_bson_size = max_bson_size;

        if( bson_find( it, out, "setName" ) ) {
            set_name = bson_iterator_string( it );
            if( strcmp( set_name, conn->replica_set->name ) != 0 ) {
                bson_destroy( out );
                conn->err = MONGO_CONN_BAD_SET_NAME;
                return MONGO_ERROR;
            }
        }
    }

    bson_destroy( out );

    if( ismaster ) {
        conn->replica_set->primary_connected = 1;
    }
    else {
        mongo_env_close_socket( conn->sock );
    }

    return MONGO_OK;
}

MONGO_EXPORT int mongo_replica_set_client( mongo *conn ) {

    int res = 0;
    mongo_host_port *node;

    conn->sock = 0;
    conn->connected = 0;

    /* First iterate over the seed nodes to get the canonical list of hosts
     * from the replica set. Break out once we have a host list.
     */
    node = conn->replica_set->seeds;
    while( node != NULL ) {
        res = mongo_env_socket_connect( conn, ( const char * )&node->host, node->port );
        if( res == MONGO_OK ) {
            mongo_replica_set_check_seed( conn );
            if( conn->replica_set->hosts )
                break;
        }
        node = node->next;
    }

    /* Iterate over the host list, checking for the primary node. */
    if( !conn->replica_set->hosts ) {
        conn->err = MONGO_CONN_NO_PRIMARY;
        return MONGO_ERROR;
    }
    else {
        node = conn->replica_set->hosts;

        while( node != NULL ) {
            res = mongo_env_socket_connect( conn, ( const char * )&node->host, node->port );

            if( res == MONGO_OK ) {
                if( mongo_replica_set_check_host( conn ) != MONGO_OK )
                    return MONGO_ERROR;

                /* Primary found, so return. */
                else if( conn->replica_set->primary_connected ) {
                    conn->primary = bson_malloc( sizeof( mongo_host_port ) );
                    snprintf( conn->primary->host, MAXHOSTNAMELEN, "%s", node->host );
                    conn->primary->port = node->port;
                    return MONGO_OK;
                }

                /* No primary, so close the connection. */
                else {
                    mongo_env_close_socket( conn->sock );
                    conn->sock = 0;
                    conn->connected = 0;
                }
            }

            node = node->next;
        }
    }


    conn->err = MONGO_CONN_NO_PRIMARY;
    return MONGO_ERROR;
}

MONGO_EXPORT int mongo_replset_connect( mongo *conn ) {
    int ret;
    bson_errprintf("WARNING: mongo_replset_connect() is deprecated, please use mongo_replica_set_client()\n");
    ret = mongo_replica_set_client( conn );
    mongo_set_write_concern( conn, 0 );
    return ret;
}

MONGO_EXPORT int mongo_set_op_timeout( mongo *conn, int millis ) {
    conn->op_timeout_ms = millis;
    if( conn->sock && conn->connected )
        mongo_env_set_socket_op_timeout( conn, millis );

    return MONGO_OK;
}

MONGO_EXPORT int mongo_reconnect( mongo *conn ) {
    int res;
    mongo_disconnect( conn );

    if( conn->replica_set ) {
        conn->replica_set->primary_connected = 0;
        mongo_replica_set_free_list( &conn->replica_set->hosts );
        conn->replica_set->hosts = NULL;
        res = mongo_replica_set_client( conn );
        return res;
    }
    else
        return mongo_env_socket_connect( conn, conn->primary->host, conn->primary->port );
}

MONGO_EXPORT int mongo_check_connection( mongo *conn ) {
    if( ! conn->connected )
        return MONGO_ERROR;

    return mongo_simple_int_command( conn, "admin", "ping", 1, NULL );
}

MONGO_EXPORT void mongo_disconnect( mongo *conn ) {
    if( ! conn->connected )
        return;

    if( conn->replica_set ) {
        conn->replica_set->primary_connected = 0;
        mongo_replica_set_free_list( &conn->replica_set->hosts );
        conn->replica_set->hosts = NULL;
    }

    mongo_env_close_socket( conn->sock );

    conn->sock = 0;
    conn->connected = 0;
}

MONGO_EXPORT void mongo_destroy( mongo *conn ) {
    mongo_disconnect( conn );

    if( conn->replica_set ) {
        mongo_replica_set_free_list( &conn->replica_set->seeds );
        mongo_replica_set_free_list( &conn->replica_set->hosts );
        bson_free( conn->replica_set->name );
        bson_free( conn->replica_set );
        conn->replica_set = NULL;
    }

    bson_free( conn->primary );

    mongo_clear_errors( conn );
}

/* Determine whether this BSON object is valid for the given operation.  */
static int mongo_bson_valid( mongo *conn, const bson *bson, int write ) {
    int size;

    if( ! bson->finished ) {
        conn->err = MONGO_BSON_NOT_FINISHED;
        return MONGO_ERROR;
    }

    size = bson_size( bson );
    if( size > conn->max_bson_size ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    if( bson->err & BSON_NOT_UTF8 ) {
        conn->err = MONGO_BSON_INVALID;
        return MONGO_ERROR;
    }

    if( write ) {
        if( ( bson->err & BSON_FIELD_HAS_DOT ) ||
                ( bson->err & BSON_FIELD_INIT_DOLLAR ) ) {

            conn->err = MONGO_BSON_INVALID;
            return MONGO_ERROR;

        }
    }

    conn->err = MONGO_CONN_SUCCESS;

    return MONGO_OK;
}

/* Determine whether this BSON object is valid for the given operation.  */
static int mongo_cursor_bson_valid( mongo_cursor *cursor, const bson *bson ) {
    if( ! bson->finished ) {
        cursor->err = MONGO_CURSOR_BSON_ERROR;
        cursor->conn->err = MONGO_BSON_NOT_FINISHED;
        return MONGO_ERROR;
    }

    if( bson->err & BSON_NOT_UTF8 ) {
        cursor->err = MONGO_CURSOR_BSON_ERROR;
        cursor->conn->err = MONGO_BSON_INVALID;
        return MONGO_ERROR;
    }

    return MONGO_OK;
}

static int mongo_check_last_error( mongo *conn, const char *ns,
                                   mongo_write_concern *write_concern ) {
    bson response[1];
    bson_iterator it[1];
    int res = 0;
    char *cmd_ns = mongo_ns_to_cmd_db( ns );

    res = mongo_find_one( conn, cmd_ns, write_concern->cmd, bson_shared_empty( ), response );
    bson_free( cmd_ns );

    if (res == MONGO_OK &&
        (bson_find( it, response, "$err" ) == BSON_STRING ||
         bson_find( it, response, "err" ) == BSON_STRING)) {

        __mongo_set_error( conn, MONGO_WRITE_ERROR,
                           "See conn->lasterrstr for details.", 0 );
        mongo_set_last_error( conn, it, response );
        res = MONGO_ERROR;
    }

    bson_destroy( response );
    return res;
}

static int mongo_choose_write_concern( mongo *conn,
                                       mongo_write_concern *custom_write_concern,
                                       mongo_write_concern **write_concern ) {

    if( custom_write_concern ) {
        *write_concern = custom_write_concern;
    }
    else if( conn->write_concern ) {
        *write_concern = conn->write_concern;
    }
    if ( *write_concern && (*write_concern)->w < 1 ) {
        *write_concern = 0; /* do not generate getLastError request */
    }
    if( *write_concern && !((*write_concern)->cmd) ) {
        __mongo_set_error( conn, MONGO_WRITE_CONCERN_INVALID,
                           "Must call mongo_write_concern_finish() before using *write_concern.", 0 );
        return MONGO_ERROR;
    }
    else
        return MONGO_OK;
}


/*********************************************************************
CRUD API
**********************************************************************/

static int mongo_message_send_and_check_write_concern( mongo *conn, const char *ns, mongo_message *mm, mongo_write_concern *write_concern ) {
   if( write_concern ) {
        if( mongo_message_send( conn, mm ) == MONGO_ERROR ) {
            return MONGO_ERROR;
        }

        return mongo_check_last_error( conn, ns, write_concern );
    }
    else {
        return mongo_message_send( conn, mm );
    }
}

MONGO_EXPORT int mongo_insert( mongo *conn, const char *ns,
                               const bson *bson, mongo_write_concern *custom_write_concern ) {

    char *data;
    mongo_message *mm;
    mongo_write_concern *write_concern = NULL;

    if( mongo_validate_ns( conn, ns ) != MONGO_OK )
        return MONGO_ERROR;

    if( mongo_bson_valid( conn, bson, 1 ) != MONGO_OK ) {
        return MONGO_ERROR;
    }

    if( mongo_choose_write_concern( conn, custom_write_concern,
                                    &write_concern ) == MONGO_ERROR ) {
        return MONGO_ERROR;
    }

    mm = mongo_message_create( 16 /* header */
                               + 4 /* ZERO */
                               + strlen( ns )
                               + 1 + bson_size( bson )
                               , 0, 0, MONGO_OP_INSERT );
    if( mm == NULL ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    data = &mm->data;
    data = mongo_data_append32( data, &ZERO );
    data = mongo_data_append( data, ns, strlen( ns ) + 1 );
    mongo_data_append( data, bson->data, bson_size( bson ) );

    return mongo_message_send_and_check_write_concern( conn, ns, mm, write_concern ); 
}

MONGO_EXPORT int mongo_insert_batch( mongo *conn, const char *ns,
                                     const bson **bsons, int count, mongo_write_concern *custom_write_concern,
                                     int flags ) {

    mongo_message *mm;
    mongo_write_concern *write_concern = NULL;
    int i;
    char *data;
    size_t overhead =  16 + 4 + strlen( ns ) + 1;
    size_t size = overhead;

    if( mongo_validate_ns( conn, ns ) != MONGO_OK )
        return MONGO_ERROR;

    for( i=0; i<count; i++ ) {
        size += bson_size( bsons[i] );
        if( mongo_bson_valid( conn, bsons[i], 1 ) != MONGO_OK )
            return MONGO_ERROR;
    }

    if( ( size - overhead ) > (size_t)conn->max_bson_size ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    if( mongo_choose_write_concern( conn, custom_write_concern,
                                    &write_concern ) == MONGO_ERROR ) {
        return MONGO_ERROR;
    }

    mm = mongo_message_create( size , 0 , 0 , MONGO_OP_INSERT );
    if( mm == NULL ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    data = &mm->data;
    if( flags & MONGO_CONTINUE_ON_ERROR )
        data = mongo_data_append32( data, &ONE );
    else
        data = mongo_data_append32( data, &ZERO );
    data = mongo_data_append( data, ns, strlen( ns ) + 1 );

    for( i=0; i<count; i++ ) {
        data = mongo_data_append( data, bsons[i]->data, bson_size( bsons[i] ) );
    }

    return mongo_message_send_and_check_write_concern( conn, ns, mm, write_concern ); 
}

MONGO_EXPORT int mongo_update( mongo *conn, const char *ns, const bson *cond,
                               const bson *op, int flags, mongo_write_concern *custom_write_concern ) {

    char *data;
    mongo_message *mm;
    mongo_write_concern *write_concern = NULL;

    /* Make sure that the op BSON is valid UTF-8.
     * TODO: decide whether to check cond as well.
     * */
    if( mongo_bson_valid( conn, ( bson * )op, 0 ) != MONGO_OK ) {
        return MONGO_ERROR;
    }

    if( mongo_choose_write_concern( conn, custom_write_concern,
                                    &write_concern ) == MONGO_ERROR ) {
        return MONGO_ERROR;
    }

    mm = mongo_message_create( 16 /* header */
                               + 4  /* ZERO */
                               + strlen( ns ) + 1
                               + 4  /* flags */
                               + bson_size( cond )
                               + bson_size( op )
                               , 0 , 0 , MONGO_OP_UPDATE );
    if( mm == NULL ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    data = &mm->data;
    data = mongo_data_append32( data, &ZERO );
    data = mongo_data_append( data, ns, strlen( ns ) + 1 );
    data = mongo_data_append32( data, &flags );
    data = mongo_data_append( data, cond->data, bson_size( cond ) );
    mongo_data_append( data, op->data, bson_size( op ) );

    return mongo_message_send_and_check_write_concern( conn, ns, mm, write_concern ); 
}

MONGO_EXPORT int mongo_remove( mongo *conn, const char *ns, const bson *cond,
                               mongo_write_concern *custom_write_concern ) {

    char *data;
    mongo_message *mm;
    mongo_write_concern *write_concern = NULL;

    /* Make sure that the BSON is valid UTF-8.
     * TODO: decide whether to check cond as well.
     * */
    if( mongo_bson_valid( conn, ( bson * )cond, 0 ) != MONGO_OK ) {
        return MONGO_ERROR;
    }

    if( mongo_choose_write_concern( conn, custom_write_concern,
                                    &write_concern ) == MONGO_ERROR ) {
        return MONGO_ERROR;
    }

    mm = mongo_message_create( 16  /* header */
                               + 4  /* ZERO */
                               + strlen( ns ) + 1
                               + 4  /* ZERO */
                               + bson_size( cond )
                               , 0 , 0 , MONGO_OP_DELETE );
    if( mm == NULL ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    data = &mm->data;
    data = mongo_data_append32( data, &ZERO );
    data = mongo_data_append( data, ns, strlen( ns ) + 1 );
    data = mongo_data_append32( data, &ZERO );
    mongo_data_append( data, cond->data, bson_size( cond ) );

    return mongo_message_send_and_check_write_concern( conn, ns, mm, write_concern ); 
}


/*********************************************************************
Write Concern API
**********************************************************************/

MONGO_EXPORT void mongo_write_concern_init( mongo_write_concern *write_concern ) {
    memset( write_concern, 0, sizeof( mongo_write_concern ) );
}

MONGO_EXPORT int mongo_write_concern_finish( mongo_write_concern *write_concern ) {
    bson *command;

    /* Destory any existing serialized write concern object and reuse it. */
    if( write_concern->cmd ) {
        bson_destroy( write_concern->cmd );
        command = write_concern->cmd;
    }
    else
        command = bson_alloc();

    if( !command ) {
        return MONGO_ERROR;
    }

    bson_init( command );

    bson_append_int( command, "getlasterror", 1 );

    if( write_concern->mode ) {
        bson_append_string( command, "w", write_concern->mode );
    }

    else if( write_concern->w && write_concern->w > 1 ) {
        bson_append_int( command, "w", write_concern->w );
    }

    if( write_concern->wtimeout ) {
        bson_append_int( command, "wtimeout", write_concern->wtimeout );
    }

    if( write_concern->j ) {
        bson_append_int( command, "j", write_concern->j );
    }

    if( write_concern->fsync ) {
        bson_append_int( command, "fsync", write_concern->fsync );
    }

    bson_finish( command );

    /* write_concern now owns the BSON command object.
     * This is freed in mongo_write_concern_destroy(). */
    write_concern->cmd = command;

    return MONGO_OK;
}

/**
 * Free the write_concern object (specifically, the BSON object that it holds).
 */
MONGO_EXPORT void mongo_write_concern_destroy( mongo_write_concern *write_concern ) {
    if( !write_concern )
        return;

    if( write_concern->cmd ) {
        bson_destroy( write_concern->cmd );
        bson_dealloc( write_concern->cmd );
        write_concern->cmd = NULL;
    }
}

MONGO_EXPORT void mongo_set_write_concern( mongo *conn,
        mongo_write_concern *write_concern ) {

    conn->write_concern = write_concern;
}

MONGO_EXPORT int mongo_write_concern_get_w( mongo_write_concern *write_concern ){    
    return write_concern->w;
}

MONGO_EXPORT int mongo_write_concern_get_wtimeout( mongo_write_concern *write_concern ){    
    return write_concern->wtimeout;
}

MONGO_EXPORT int mongo_write_concern_get_j( mongo_write_concern *write_concern ){    
    return write_concern->j;
}

MONGO_EXPORT int mongo_write_concern_get_fsync( mongo_write_concern *write_concern ){    
    return write_concern->fsync;
}

MONGO_EXPORT const char* mongo_write_concern_get_mode( mongo_write_concern *write_concern ){    
    return write_concern->mode;
}

MONGO_EXPORT bson* mongo_write_concern_get_cmd( mongo_write_concern *write_concern ){    
    return write_concern->cmd;
}

MONGO_EXPORT void mongo_write_concern_set_w( mongo_write_concern *write_concern, int w ){    
    write_concern->w = w;
}

MONGO_EXPORT void mongo_write_concern_set_wtimeout( mongo_write_concern *write_concern, int wtimeout ){    
    write_concern->wtimeout = wtimeout;

}

MONGO_EXPORT void mongo_write_concern_set_j( mongo_write_concern *write_concern, int j ){    
    write_concern->j = j;
}

MONGO_EXPORT void mongo_write_concern_set_fsync( mongo_write_concern *write_concern, int fsync ){    
    write_concern->fsync = fsync;

}

MONGO_EXPORT void mongo_write_concern_set_mode( mongo_write_concern *write_concern, const char* mode ){    
    write_concern->mode = mode;
}

static int mongo_cursor_op_query( mongo_cursor *cursor ) {
    int res;
    char *data;
    mongo_message *mm;
    bson temp;
    bson_iterator it;

    /* Clear any errors. */
    mongo_clear_errors( cursor->conn );

    /* Set up default values for query and fields, if necessary. */
    if( ! cursor->query )
        cursor->query = bson_shared_empty( );
    else if( mongo_cursor_bson_valid( cursor, cursor->query ) != MONGO_OK )
        return MONGO_ERROR;

    if( ! cursor->fields )
        cursor->fields = bson_shared_empty( );
    else if( mongo_cursor_bson_valid( cursor, cursor->fields ) != MONGO_OK )
        return MONGO_ERROR;

    mm = mongo_message_create( 16 + /* header */
                               4 + /*  options */
                               strlen( cursor->ns ) + 1 + /* ns */
                               4 + 4 + /* skip,return */
                               bson_size( cursor->query ) +
                               bson_size( cursor->fields ) ,
                               0 , 0 , MONGO_OP_QUERY );
    if( mm == NULL ) {
        return MONGO_ERROR;
    }

    data = &mm->data;
    data = mongo_data_append32( data , &cursor->options );
    data = mongo_data_append( data , cursor->ns , strlen( cursor->ns ) + 1 );
    data = mongo_data_append32( data , &cursor->skip );
    data = mongo_data_append32( data , &cursor->limit );
    data = mongo_data_append( data , cursor->query->data , bson_size( cursor->query ) );
    if ( cursor->fields )
        data = mongo_data_append( data , cursor->fields->data , bson_size( cursor->fields ) );

    bson_fatal_msg( ( data == ( ( char * )mm ) + mm->head.len ), "query building fail!" );

    res = mongo_message_send( cursor->conn , mm );
    if( res != MONGO_OK ) {
        return MONGO_ERROR;
    }

    res = mongo_read_response( cursor->conn, ( mongo_reply ** )&( cursor->reply ) );
    if( res != MONGO_OK ) {
        return MONGO_ERROR;
    }

    if( cursor->reply->fields.num == 1 ) {
        bson_init_finished_data( &temp, &cursor->reply->objs, 0 );
        if( bson_find( &it, &temp, "$err" ) ) {
            mongo_set_last_error( cursor->conn, &it, &temp );
            cursor->err = MONGO_CURSOR_QUERY_FAIL;
            return MONGO_ERROR;
        }
    }

    cursor->seen += cursor->reply->fields.num;
    cursor->flags |= MONGO_CURSOR_QUERY_SENT;
    return MONGO_OK;
}

static int mongo_cursor_get_more( mongo_cursor *cursor ) {
    int res;

    if( cursor->limit > 0 && cursor->seen >= cursor->limit ) {
        cursor->err = MONGO_CURSOR_EXHAUSTED;
        return MONGO_ERROR;
    }
    else if( ! cursor->reply ) {
        cursor->err = MONGO_CURSOR_INVALID;
        return MONGO_ERROR;
    }
    else if( ! cursor->reply->fields.cursorID ) {
        cursor->err = MONGO_CURSOR_EXHAUSTED;
        return MONGO_ERROR;
    }
    else {
        char *data;
        size_t sl = strlen( cursor->ns )+1;
        int limit = 0;
        mongo_message *mm;

        if( cursor->limit > 0 )
            limit = cursor->limit - cursor->seen;

        mm = mongo_message_create( 16 /*header*/
                                   +4 /*ZERO*/
                                   +sl
                                   +4 /*numToReturn*/
                                   +8 /*cursorID*/
                                   , 0, 0, MONGO_OP_GET_MORE );
        if( mm == NULL ) {
            return MONGO_ERROR;
        }

        data = &mm->data;
        data = mongo_data_append32( data, &ZERO );
        data = mongo_data_append( data, cursor->ns, sl );
        data = mongo_data_append32( data, &limit );
        mongo_data_append64( data, &cursor->reply->fields.cursorID );

        bson_free( cursor->reply );
        res = mongo_message_send( cursor->conn, mm );
        if( res != MONGO_OK ) {
            mongo_cursor_destroy( cursor );
            return MONGO_ERROR;
        }

        res = mongo_read_response( cursor->conn, &( cursor->reply ) );
        if( res != MONGO_OK ) {
            mongo_cursor_destroy( cursor );
            return MONGO_ERROR;
        }
        cursor->current.data = NULL;
        cursor->seen += cursor->reply->fields.num;

        return MONGO_OK;
    }
}

MONGO_EXPORT mongo_cursor *mongo_find( mongo *conn, const char *ns, const bson *query,
                                       const bson *fields, int limit, int skip, int options ) {

    mongo_cursor *cursor = mongo_cursor_alloc();
    mongo_cursor_init( cursor, conn, ns );
    cursor->flags |= MONGO_CURSOR_MUST_FREE;

    mongo_cursor_set_query( cursor, query );
    mongo_cursor_set_fields( cursor, fields );
    mongo_cursor_set_limit( cursor, limit );
    mongo_cursor_set_skip( cursor, skip );
    mongo_cursor_set_options( cursor, options );

    if( mongo_cursor_op_query( cursor ) == MONGO_OK )
        return cursor;
    else {
        mongo_cursor_destroy( cursor );
        return NULL;
    }
}

MONGO_EXPORT int mongo_find_one( mongo *conn, const char *ns, const bson *query,
                                 const bson *fields, bson *out ) {
    int ret;
    mongo_cursor cursor[1];
    mongo_cursor_init( cursor, conn, ns );
    mongo_cursor_set_query( cursor, query );
    mongo_cursor_set_fields( cursor, fields );
    mongo_cursor_set_limit( cursor, 1 );

    ret = mongo_cursor_next(cursor);
    if (ret == MONGO_OK && out)
        ret = bson_copy(out, &cursor->current);
    if (ret != MONGO_OK && out)
        bson_init_zero(out);

    mongo_cursor_destroy( cursor );
    return ret;
}

MONGO_EXPORT void mongo_cursor_init( mongo_cursor *cursor, mongo *conn, const char *ns ) {
    memset( cursor, 0, sizeof( mongo_cursor ) );
    cursor->conn = conn;
    cursor->ns = ( const char * )bson_malloc( strlen( ns ) + 1 );
    strncpy( ( char * )cursor->ns, ns, strlen( ns ) + 1 );
    cursor->current.data = NULL;
}

MONGO_EXPORT void mongo_cursor_set_query( mongo_cursor *cursor, const bson *query ) {
    cursor->query = query;
}

MONGO_EXPORT void mongo_cursor_set_fields( mongo_cursor *cursor, const bson *fields ) {
    cursor->fields = fields;
}

MONGO_EXPORT void mongo_cursor_set_skip( mongo_cursor *cursor, int skip ) {
    cursor->skip = skip;
}

MONGO_EXPORT void mongo_cursor_set_limit( mongo_cursor *cursor, int limit ) {
    cursor->limit = limit;
}

MONGO_EXPORT void mongo_cursor_set_options( mongo_cursor *cursor, int options ) {
    cursor->options = options;
}

MONGO_EXPORT const char *mongo_cursor_data( mongo_cursor *cursor ) {
    return cursor->current.data;
}

MONGO_EXPORT const bson *mongo_cursor_bson( mongo_cursor *cursor ) {
    return (const bson *)&(cursor->current);
}

MONGO_EXPORT int mongo_cursor_next( mongo_cursor *cursor ) {
    char *next_object;
    char *message_end;

    if( cursor == NULL ) return MONGO_ERROR;

    if( ! ( cursor->flags & MONGO_CURSOR_QUERY_SENT ) )
        if( mongo_cursor_op_query( cursor ) != MONGO_OK )
            return MONGO_ERROR;

    if( !cursor->reply )
        return MONGO_ERROR;

    /* no data */
    if ( cursor->reply->fields.num == 0 ) {

        /* Special case for tailable cursors. */
        if( cursor->reply->fields.cursorID ) {
            if( ( mongo_cursor_get_more( cursor ) != MONGO_OK ) ||
                    cursor->reply->fields.num == 0 ) {
                return MONGO_ERROR;
            }
        }

        else
            return MONGO_ERROR;
    }

    /* first */
    if ( cursor->current.data == NULL ) {
        bson_init_finished_data( &cursor->current, &cursor->reply->objs, 0 );
        return MONGO_OK;
    }

    next_object = cursor->current.data + bson_size( &cursor->current );
    message_end = ( char * )cursor->reply + cursor->reply->head.len;

    if ( next_object >= message_end ) {
        if( mongo_cursor_get_more( cursor ) != MONGO_OK )
            return MONGO_ERROR;

        if ( cursor->reply->fields.num == 0 ) {
            /* Special case for tailable cursors. */
            if ( cursor->reply->fields.cursorID ) {
                cursor->err = MONGO_CURSOR_PENDING;
                return MONGO_ERROR;
            }
            else
                return MONGO_ERROR;
        }

        bson_init_finished_data( &cursor->current, &cursor->reply->objs, 0 );
    }
    else {
        bson_init_finished_data( &cursor->current, next_object, 0 );
    }

    return MONGO_OK;
}

MONGO_EXPORT int mongo_cursor_destroy( mongo_cursor *cursor ) {
    int result = MONGO_OK;
    char *data;

    if ( !cursor ) return result;

    /* Kill cursor if live. */
    if ( cursor->reply && cursor->reply->fields.cursorID ) {
        mongo *conn = cursor->conn;
        mongo_message *mm = mongo_message_create( 16 /*header*/
                            +4 /*ZERO*/
                            +4 /*numCursors*/
                            +8 /*cursorID*/
                            , 0, 0, MONGO_OP_KILL_CURSORS );
        if( mm == NULL ) {
            return MONGO_ERROR;
        }
        data = &mm->data;
        data = mongo_data_append32( data, &ZERO );
        data = mongo_data_append32( data, &ONE );
        mongo_data_append64( data, &cursor->reply->fields.cursorID );

        result = mongo_message_send( conn, mm );
    }

    bson_free( cursor->reply );
    bson_free( ( void * )cursor->ns );

    if( cursor->flags & MONGO_CURSOR_MUST_FREE )
        bson_free( cursor );

    return result;
}

/* MongoDB Helper Functions */

#define INDEX_NAME_BUFFER_SIZE 255
#define INDEX_NAME_MAX_LENGTH (INDEX_NAME_BUFFER_SIZE - 1)

MONGO_EXPORT int mongo_create_index( mongo *conn, const char *ns, const bson *key, const char *name, int options, bson *out ) {
    bson b;
    bson_iterator it;
    char default_name[INDEX_NAME_BUFFER_SIZE] = {'\0'};
    size_t len = 0;
    size_t remaining;
    char idxns[1024];
    char *p = NULL;

    if ( !name ) {
        bson_iterator_init( &it, key );
        while( len < INDEX_NAME_MAX_LENGTH && bson_iterator_next( &it ) ) {
            remaining = INDEX_NAME_MAX_LENGTH - len;
            strncat( default_name, bson_iterator_key( &it ), remaining );
            len = strlen( default_name );
            remaining = INDEX_NAME_MAX_LENGTH - len;
            strncat( default_name, ( bson_iterator_int( &it ) < 0 ) ? "_-1" : "_1", remaining );
            len = strlen( default_name );
        }
    }

    bson_init( &b );
    bson_append_bson( &b, "key", key );
    bson_append_string( &b, "ns", ns );
    bson_append_string( &b, "name", name ? name : default_name );
    if ( options & MONGO_INDEX_UNIQUE )
        bson_append_bool( &b, "unique", 1 );
    if ( options & MONGO_INDEX_DROP_DUPS )
        bson_append_bool( &b, "dropDups", 1 );
    if ( options & MONGO_INDEX_BACKGROUND )
        bson_append_bool( &b, "background", 1 );
    if ( options & MONGO_INDEX_SPARSE )
        bson_append_bool( &b, "sparse", 1 );
    bson_finish( &b );

    strncpy( idxns, ns, 1024-16 );
    p = strchr( idxns, '.' );
    if ( !p ) {
	    bson_destroy( &b );
	    return MONGO_ERROR;
    }
    strcpy( p, ".system.indexes" );
    if ( mongo_insert( conn, idxns, &b, NULL ) != MONGO_OK) {
	    bson_destroy( &b );
	    return MONGO_ERROR;
    }
    bson_destroy( &b );

    *strchr( idxns, '.' ) = '\0'; /* just db not ns */
    return mongo_cmd_get_last_error( conn, idxns, out );
}

MONGO_EXPORT bson_bool_t mongo_create_simple_index( mongo *conn, const char *ns, const char *field, int options, bson *out ) {
    bson b[1];
    bson_bool_t success;

    bson_init( b );
    bson_append_int( b, field, 1 );
    bson_finish( b );

    success = mongo_create_index( conn, ns, b, NULL, options, out );
    bson_destroy( b );
    return success;
}

MONGO_EXPORT int mongo_create_capped_collection( mongo *conn, const char *db,
        const char *collection, int size, int max, bson *out ) {

    bson b[1];
    int result;

    bson_init( b );
    bson_append_string( b, "create", collection );
    bson_append_bool( b, "capped", 1 );
    bson_append_int( b, "size", size );
    if( max > 0 )
        bson_append_int( b, "max", size );
    bson_finish( b );

    result = mongo_run_command( conn, db, b, out );

    bson_destroy( b );

    return result;
}

MONGO_EXPORT double mongo_count( mongo *conn, const char *db, const char *coll, const bson *query ) {
    bson cmd[1];
    bson out[1];
    double count = MONGO_ERROR;  // -1

    bson_init( cmd );
    bson_append_string( cmd, "count", coll );
    if ( query && bson_size( query ) > 5 ) /* not empty */
        bson_append_bson( cmd, "query", query );
    bson_finish( cmd );

    if( mongo_run_command( conn, db, cmd, out ) == MONGO_OK ) {
        bson_iterator it[1];
        if( bson_find( it, out, "n" ) )
            count = bson_iterator_double( it );
    }
    bson_destroy( out );
    bson_destroy( cmd );
    return count;
}

MONGO_EXPORT int mongo_run_command( mongo *conn, const char *db, const bson *command,
                                    bson *out ) {
    bson response[1];
    bson_iterator it[1];
    size_t sl = strlen( db );
    char *ns = (char*) bson_malloc( sl + 5 + 1 ); /* ".$cmd" + nul */
    int res = 0;

    strcpy( ns, db );
    strcpy( ns+sl, ".$cmd" );

    res = mongo_find_one( conn, ns, command, bson_shared_empty( ), response );
    bson_free( ns );

    if (res == MONGO_OK && (!bson_find( it, response, "ok" ) || !bson_iterator_bool( it )) ) {
        conn->err = MONGO_COMMAND_FAILED;
        bson_destroy( response );
        res = MONGO_ERROR;
    }

    if (out)
        if (res == MONGO_OK)
            *out = *response;
        else
            bson_init_zero(out);
    else if (res == MONGO_OK)
        bson_destroy(response);

    return res;
}

MONGO_EXPORT int mongo_simple_int_command( mongo *conn, const char *db,
        const char *cmdstr, int arg, bson *out ) {

    bson cmd[1];
    int result;

    bson_init( cmd );
    bson_append_int( cmd, cmdstr, arg );
    bson_finish( cmd );

    result = mongo_run_command( conn, db, cmd, out );

    bson_destroy( cmd );

    return result;
}

MONGO_EXPORT int mongo_simple_str_command( mongo *conn, const char *db,
        const char *cmdstr, const char *arg, bson *out ) {

    int result;
    bson cmd;
    bson_init( &cmd );
    bson_append_string( &cmd, cmdstr, arg );
    bson_finish( &cmd );

    result = mongo_run_command( conn, db, &cmd, out );

    bson_destroy( &cmd );

    return result;
}

MONGO_EXPORT int mongo_cmd_drop_db( mongo *conn, const char *db ) {
    return mongo_simple_int_command( conn, db, "dropDatabase", 1, NULL );
}

MONGO_EXPORT int mongo_cmd_drop_collection( mongo *conn, const char *db, const char *collection, bson *out ) {
    return mongo_simple_str_command( conn, db, "drop", collection, out );
}

MONGO_EXPORT void mongo_cmd_reset_error( mongo *conn, const char *db ) {
    mongo_simple_int_command( conn, db, "reseterror", 1, NULL );
}

static int mongo_cmd_get_error_helper( mongo *conn, const char *db,
                                       bson *realout, const char *cmdtype ) {

    bson out[1];
    bson_bool_t haserror = 0;

    /* Reset last error codes. */
    mongo_clear_errors( conn );
    bson_init_zero(out);

    /* If there's an error, store its code and string in the connection object. */
    if( mongo_simple_int_command( conn, db, cmdtype, 1, out ) == MONGO_OK ) {
        bson_iterator it[1];
        haserror = ( bson_find( it, out, "err" ) != BSON_NULL );
        if( haserror ) mongo_set_last_error( conn, it, out );
    }

    if( realout )
        *realout = *out; /* transfer of ownership */
    else
        bson_destroy( out );

    if( haserror )
        return MONGO_ERROR;
    else
        return MONGO_OK;
}

MONGO_EXPORT int mongo_cmd_get_prev_error( mongo *conn, const char *db, bson *out ) {
    return mongo_cmd_get_error_helper( conn, db, out, "getpreverror" );
}

MONGO_EXPORT int mongo_cmd_get_last_error( mongo *conn, const char *db, bson *out ) {
    return mongo_cmd_get_error_helper( conn, db, out, "getlasterror" );
}

MONGO_EXPORT bson_bool_t mongo_cmd_ismaster( mongo *conn, bson *realout ) {
    bson out[1];
    bson_bool_t ismaster = 0;

    int res = mongo_simple_int_command( conn, "admin", "ismaster", 1, out );
    if (res == MONGO_OK) {
        bson_iterator it[1];
        if (bson_find( it, out, "ismaster") != BSON_EOO )
            ismaster = bson_iterator_bool( it );
        if (realout)
            *realout = *out; /* transfer of ownership */
        else
            bson_destroy( out );
    } else if (realout)
        bson_init_zero(realout);

    return ismaster;
}

static int mongo_pass_digest( mongo *conn, const char *user, const char *pass, char hex_digest[33] ) {

    if( strlen( user ) >= INT32_MAX || strlen( pass ) >= INT32_MAX ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    {   DIGEST_CTX ctx = rpmDigestInit(PGPHASHALGO_MD5, RPMDIGEST_NONE);
        const char * _digest = NULL;
        int xx;
        xx = rpmDigestUpdate(ctx, user, strlen(user));
        xx = rpmDigestUpdate(ctx, ":mongo:", 7);
        xx = rpmDigestUpdate(ctx, pass, strlen(pass));
        xx = rpmDigestFinal(ctx, &_digest, NULL, 1);
        strncpy(hex_digest, _digest, 32+1);
        _digest = _free(_digest);
    }

    return MONGO_OK;
}

MONGO_EXPORT int mongo_cmd_add_user( mongo *conn, const char *db, const char *user, const char *pass ) {
    bson user_obj;
    bson pass_obj;
    char hex_digest[33];
    char *ns = bson_malloc( strlen( db ) + strlen( ".system.users" ) + 1 );
    int res;

    strcpy( ns, db );
    strcpy( ns+strlen( db ), ".system.users" );

    res = mongo_pass_digest( conn, user, pass, hex_digest );
    if (res != MONGO_OK) {
        free(ns);
        return res;
    }

    bson_init( &user_obj );
    bson_append_string( &user_obj, "user", user );
    bson_finish( &user_obj );

    bson_init( &pass_obj );
    bson_append_start_object( &pass_obj, "$set" );
    bson_append_string( &pass_obj, "pwd", hex_digest );
    bson_append_finish_object( &pass_obj );
    bson_finish( &pass_obj );

    res = mongo_update( conn, ns, &user_obj, &pass_obj, MONGO_UPDATE_UPSERT, NULL );

    bson_free( ns );
    bson_destroy( &user_obj );
    bson_destroy( &pass_obj );

    return res;
}

MONGO_EXPORT bson_bool_t mongo_cmd_authenticate( mongo *conn, const char *db, const char *user, const char *pass ) {
    bson from_db;
    bson cmd;
    const char *nonce;
    int result;

    char hex_digest[32+1];

    if( mongo_simple_int_command( conn, db, "getnonce", 1, &from_db ) == MONGO_OK ) {
        bson_iterator it;
        if (bson_find(&it, &from_db, "nonce") != BSON_EOO )
            nonce = bson_iterator_string( &it );
	else
            return MONGO_ERROR;
    } else {
        return MONGO_ERROR;
    }

    result = mongo_pass_digest( conn, user, pass, hex_digest );
    if( result != MONGO_OK ) {
        return result;
    }

    if( strlen( nonce ) >= INT32_MAX || strlen( user ) >= INT32_MAX ) {
        conn->err = MONGO_BSON_TOO_LARGE;
        return MONGO_ERROR;
    }

    {	DIGEST_CTX ctx = rpmDigestInit(PGPHASHALGO_MD5, RPMDIGEST_NONE);
	const char * _digest = NULL;
	int xx;
	xx = rpmDigestUpdate(ctx, nonce, strlen(nonce));
	xx = rpmDigestUpdate(ctx, user, strlen(user));
	xx = rpmDigestUpdate(ctx, hex_digest, 32);
	xx = rpmDigestFinal(ctx, &_digest, NULL, 1);
	strncpy(hex_digest, _digest, 32+1);
	hex_digest[32] = '\0';
	_digest = _free(_digest);
    }

    bson_init( &cmd );
    bson_append_int( &cmd, "authenticate", 1 );
    bson_append_string( &cmd, "user", user );
    bson_append_string( &cmd, "nonce", nonce );
    bson_append_string( &cmd, "key", hex_digest );
    bson_finish( &cmd );

    result = mongo_run_command( conn, db, &cmd, NULL );

    bson_destroy( &from_db );
    bson_destroy( &cmd );

    return result;
}

/*==============================================================*/

static void rpmmgoFini(void * _mgo)
	/*@globals fileSystem @*/
	/*@modifies *_mgo, fileSystem @*/
{
    rpmmgo mgo = (rpmmgo) _mgo;

    mgo->fn = _free(mgo->fn);
}

/*@unchecked@*/ /*@only@*/ /*@null@*/
rpmioPool _rpmmgoPool = NULL;

static rpmmgo rpmmgoGetPool(/*@null@*/ rpmioPool pool)
	/*@globals _rpmmgoPool, fileSystem @*/
	/*@modifies pool, _rpmmgoPool, fileSystem @*/
{
    rpmmgo mgo;

    if (_rpmmgoPool == NULL) {
	_rpmmgoPool = rpmioNewPool("mgo", sizeof(*mgo), -1, _rpmmgo_debug,
			NULL, NULL, rpmmgoFini);
	pool = _rpmmgoPool;
    }
    mgo = (rpmmgo) rpmioGetPool(pool, sizeof(*mgo));
    memset(((char *)mgo)+sizeof(mgo->_item), 0, sizeof(*mgo)-sizeof(mgo->_item));
    return mgo;
}

rpmmgo rpmmgoNew(const char * fn, int flags)
{
    rpmmgo mgo = rpmmgoGetPool(_rpmmgoPool);

    if (fn)
	mgo->fn = xstrdup(fn);

    return rpmmgoLink(mgo);
}
