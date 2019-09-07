filename="mydatafile.dat"
**write a command to gzip the file

today_dt=`date +%Y%m%d`

#check if the directory already exists and capture the return code
hadoop fs -ls /data/archive/$today_dt

if [ $? != 0 ]
then
  #Directory doesn't exists
  **write command to create directory
else
  #Directory exists. Now lets check if file exists. If so then delete it
  hadoop fs -ls /data/archive/$today_dt/${filename}.gz
  if [$? != 0]
  then
    **write a command to delete that file
  fi
fi

#Ok, so now we are all set with initial checks. Lets push the file from local to HDFS
**write a command to move file from Local file system to HDFS

