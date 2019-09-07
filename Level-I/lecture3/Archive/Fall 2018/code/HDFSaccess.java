Path output = new Path(args[1]);
FileSystem hdfs = FileSystem.get(conf);

if (hdfs.exists(output)) {
	hdfs.delete(output, true);
}