 #include "hdfs.h"
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <sys/stat.h>
 #include <sys/time.h>
 #include <inttypes.h>

 hdfsFile openFile(hdfsFS fs, const char *path, int flags, int bufferSize, short replication, tSize blockSize){
     hdfsFile file = hdfsOpenFile(fs, path, flags, 0, 0, 0);
     if(!file ) {
           fprintf(stderr, "Failed to open %s file!\n", path);
           exit(-1);
     }
     return file;
 }

 void permission_disp(short permissions, char *rtr)
 {
     rtr[9] = '\0';
     int i;
     short perm;
     for(i = 2; i >= 0; i--)
     {
         perm = permissions >> (i * 3);
         rtr[0] = perm & 4 ? 'r' : '-';
         rtr[1] = perm & 2 ? 'w' : '-';
         rtr[2] = perm & 1 ? 'x' : '-';
         rtr += 3;
     }
 }

 void printFileInfo(hdfsFileInfo *fileInfo){
       fprintf(stderr, "\nName: %s, ", fileInfo->mName);
       fprintf(stderr, "Type: %c, ", (char)(fileInfo->mKind));
       fprintf(stderr, "Replication: %d, ", fileInfo->mReplication);
       fprintf(stderr, "BlockSize: %" PRId64 ", ", fileInfo->mBlockSize);
       fprintf(stderr, "Size: %" PRId64 ", ", fileInfo->mSize);
       fprintf(stderr, "LastMod: %" PRId64 ", ", fileInfo->mLastMod);
       fprintf(stderr, "Owner: %s, ", fileInfo->mOwner);
       fprintf(stderr, "Group: %s, ", fileInfo->mGroup);
       char permissions[10];
       permission_disp(fileInfo->mPermissions, permissions);
       fprintf(stderr, "Permissions: %d (%s)\n",
                fileInfo->mPermissions, permissions);
 }

 int main(int argc, char **argv) {

     hdfsFS fs = hdfsConnect("default", 0);
     fprintf(stderr, "hdfsConnect- SUCCESS!\n");

     const char* dir = "/tmp/nativeTest";
     int exitCode = hdfsCreateDirectory(fs, dir);
     if( exitCode == -1 ){
     fprintf(stderr, "Failed to create directory %s \n", dir );
     exit(-1);
     }
     fprintf(stderr, "hdfsCreateDirectory- SUCCESS! : %s\n", dir);


     //Write File
     const char* file = "/tmp/nativeTest/testfile.txt";
     hdfsFile writeFile = openFile(fs, (char*)file, O_WRONLY |O_CREAT, 0, 0, 0);
     fprintf(stderr, "hdfsOpenFile- SUCCESS! for write : %s\n", file);


     if(!hdfsFileIsOpenForWrite(writeFile)){
       fprintf(stderr, "Failed to open %s for writing.\n", file);
           exit(-1);
     }

     char* buffer = "Hadoop HDFS Native file write!";

     hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer)+1);
     fprintf(stderr, "hdfsWrite- SUCCESS! : %s\n", file);

     printf("Flushing file data ....\n");
     if (hdfsFlush(fs, writeFile)) {
           fprintf(stderr, "Failed to 'flush' %s\n", file);
           exit(-1);
     }
     hdfsCloseFile(fs, writeFile);
     fprintf(stderr, "hdfsCloseFile- SUCCESS! : %s\n", file);

      //Read File
     hdfsFile readFile = openFile(fs, (char*)file, O_RDONLY, 100, 0, 0);
     fprintf(stderr, "hdfsOpenFile- SUCCESS! for read : %s\n", file);

     if(!hdfsFileIsOpenForRead(readFile)){
       fprintf(stderr, "Failed to open %s for reading.\n", file);
           exit(-1);
     }

     buffer = (char *) malloc(100);
     tSize num_read = hdfsRead(fs, readFile, (void*)buffer, 100);
     fprintf(stderr, "hdfsRead- SUCCESS!, Byte read : %d, File contant : %s \n", num_read ,buffer);
     hdfsCloseFile(fs, readFile);

     //HDFS seek
     buffer = (char *) malloc(100);
     readFile = openFile(fs, file, O_RDONLY, 100, 0, 0);
     if (hdfsSeek(fs, readFile, 10)) {
           fprintf(stderr, "Failed to 'seek' %s\n", file);
           exit(-1);
     }
     num_read = hdfsRead(fs, readFile, (void*)buffer, 100);
     fprintf(stderr, "hdfsSeek- SUCCESS!, Byte read : %d, File seek contant : %s \n", num_read ,buffer);
     hdfsCloseFile(fs, readFile);

     //HDFS Pread
     buffer = (char *) malloc(100);
     readFile = openFile(fs, file, O_RDONLY, 100, 0, 0);
     num_read = hdfsPread(fs, readFile, 10,(void*)buffer, 10);
     fprintf(stderr, "hdfsPread- SUCCESS!, Byte read : %d, File pead contant : %s \n", num_read ,buffer);
     hdfsCloseFile(fs, readFile);


     //Copy File
     const char* destfile = "/tmp/nativeTest/testfile1.txt";
     if (hdfsCopy(fs, file, fs, destfile)) {
           fprintf(stderr, "File copy failed, src : %s, des : %s \n", file, destfile);
           exit(-1);
     }
     fprintf(stderr, "hdfsCopy- SUCCESS!, File copied, src : %s, des : %s \n", file, destfile);

     //Move File
     const char* mvfile = "/tmp/nativeTest/testfile2.txt";
     if (hdfsMove(fs, destfile, fs, mvfile )) {
           fprintf(stderr, "File move failed, src : %s, des : %s \n", destfile , mvfile);
           exit(-1);
     }
     fprintf(stderr, "hdfsMove- SUCCESS!, File moved, src : %s, des : %s \n", destfile , mvfile);

     //Rename File
     const char* renamefile = "/tmp/nativeTest/testfile3.txt";
     if (hdfsRename(fs, mvfile, renamefile)) {
           fprintf(stderr, "File rename failed, Old name : %s, New name : %s \n", mvfile, renamefile);
           exit(-1);
     }
     fprintf(stderr, "hdfsRename- SUCCESS!, File renamed, Old name : %s, New name : %s \n", mvfile, renamefile);

     //Delete File
     if (hdfsDelete(fs, renamefile, 0)) {
           fprintf(stderr, "File delete failed : %s \n", renamefile);
           exit(-1);
     }
     fprintf(stderr, "hdfsDelete- SUCCESS!, File deleted : %s\n",renamefile);

     // Set replica
     if (hdfsSetReplication(fs, file, 10)) {
           fprintf(stderr, "Failed to set replication : %s \n", file );
           exit(-1);
     }
     fprintf(stderr, "hdfsSetReplication- SUCCESS!, Set replication 10 for %s\n",file);

     // Set chown
     if (hdfsChown(fs, file, "root", "root")) {
           fprintf(stderr, "Failed to set chown : %s \n", file );
           exit(-1);
     }
     fprintf(stderr, "hdfsChown- SUCCESS!, Chown success for %s\n",file);

     // Set chmod
     if (hdfsChmod(fs, file, S_IRWXU | S_IRWXG | S_IRWXO)) {
           fprintf(stderr, "Failed to set chmod: %s \n", file );
           exit(-1);
     }
     fprintf(stderr, "hdfsChmod- SUCCESS!, Chmod success for %s\n",file);

     //Set time for file
      struct timeval now;
      gettimeofday(&now, NULL);
      if (hdfsUtime(fs, file, now.tv_sec, now.tv_sec)) {
           fprintf(stderr, "Failed to set time: %s \n", file );
           exit(-1);
     }
     fprintf(stderr, "hdfsUtime- SUCCESS!, Set time success for %s\n",file);

     //File Info
      hdfsFileInfo *fileInfo = NULL;
      if((fileInfo = hdfsGetPathInfo(fs, file)) != NULL) {
      printFileInfo(fileInfo);
          hdfsFreeFileInfo(fileInfo, 1);
          fprintf(stderr, "hdfsGetPathInfo - SUCCESS!\n");
     }

     // hdfsListDirectory
     hdfsFileInfo *fileList = 0;
     int numEntries = 0;
     if((fileList = hdfsListDirectory(fs, dir, &numEntries)) != NULL) {
        int i = 0;
        for(i=0; i < numEntries; ++i) {
           printFileInfo(fileList+i);
        }
        hdfsFreeFileInfo(fileList, numEntries);
     }
     fprintf(stderr, "hdfsListDirectory- SUCCESS!, %s\n", dir);

     //Truncat
     tOffset offset=20;
     hdfsTruncateFile(fs, file, offset);
     fprintf(stderr, "hdfsTruncateFile- SUCCESS!, %s\n", file);

     printf("Block Size : %" PRId64 " \n",hdfsGetDefaultBlockSize(fs));
     fprintf(stderr, "hdfsGetDefaultBlockSize- SUCCESS!\n");

     printf("Block Size : %" PRId64 " for file %s\n",hdfsGetDefaultBlockSizeAtPath(fs,file),file);
     fprintf(stderr, "hdfsGetDefaultBlockSizeAtPath- SUCCESS!\n");

     printf("HDFS Capacity : %" PRId64 "\n",hdfsGetCapacity(fs));
     fprintf(stderr, "hdfsGetCapacity- SUCCESS!\n");

     printf("HDFS Used : %" PRId64 "\n",hdfsGetUsed(fs));
     fprintf(stderr, "hdfsGetCapacity- SUCCESS!\n");

     if (hdfsExists(fs, file)) {
           fprintf(stderr, "File %s not exist\n", file );
           exit(-1);
     }
     fprintf(stderr, "hdfsExists- SUCCESS! %s\n", file);

     char *val = NULL;
     hdfsConfGetStr("fs.defaultFS", &val);
     fprintf(stderr, "hdfsConfGetStr- SUCCESS : %s \n", val);

     //Stream Builder API
     buffer = (char *) malloc(100);
     struct hdfsStreamBuilder *builder= hdfsStreamBuilderAlloc(fs, (char*)file, O_RDONLY);
     hdfsStreamBuilderSetBufferSize(builder,100);
     hdfsStreamBuilderSetReplication(builder,20);
     hdfsStreamBuilderSetDefaultBlockSize(builder,10485760);
     readFile = hdfsStreamBuilderBuild(builder);
     num_read = hdfsRead(fs, readFile, (void*)buffer, 100);
     fprintf(stderr, "hdfsStreamBuilderBuild- SUCCESS! File read success. Byte read : %d, File contant : %s \n", num_read ,buffer);
     free(buffer);

     struct hdfsReadStatistics *stats = NULL;
     hdfsFileGetReadStatistics(readFile, &stats);
     fprintf(stderr, "hdfsFileGetReadStatistics- SUCCESS! totalBytesRead : %" PRId64 ", totalLocalBytesRead : %" PRId64 ", totalShortCircuitBytesRead : %" PRId64 ", totalZeroCopyBytesRead : %" PRId64 "\n", stats->totalBytesRead , stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead,  stats->totalZeroCopyBytesRead);
     hdfsFileFreeReadStatistics(stats);

     hdfsDisconnect(fs);
     return 0;
 }

