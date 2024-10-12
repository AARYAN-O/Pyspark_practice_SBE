# fs means file system

dbutils.fs.rm("dbfs:/output/new_data",True)

# What does recurse=True mean ?
# It means that the files within the folder that we have specified using the directory path needs to be deleted.

# So it deletes everything inside the target folder .

# Now , in case , the recurse=False , then if the directory is empty , then it will throw an error.Otherwise , it will
# not throw an error .

