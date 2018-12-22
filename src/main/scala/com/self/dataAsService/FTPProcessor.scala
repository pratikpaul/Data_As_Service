package com.self.dataAsService

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTP
import java.io.FileInputStream
import java.io.File
import java.io.InputStream

object FTPProcessor {

  def ftpUpload(ftpHost: String, ftpUser: String, ftpPassword: String, localFilePath: String, remotePath: String): Boolean = {
    val ftpClient = new FTPClient

    ftpClient.connect(ftpHost)
    ftpClient.login(ftpUser, ftpPassword)

    val localFile = new File(localFilePath);
    val localFileName = localFile.getName

    val remoteFilePath = remotePath.endsWith("/") match {
      case true => { remotePath + localFileName }
      case _ => { remotePath + "/" + localFileName }
    }

    println("localFilepath: " + localFilePath)
    println("remoteFilePath: " + remoteFilePath)

    val inputStream = new FileInputStream(localFile);
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
    val returnResp = ftpClient.storeFile(remoteFilePath, inputStream);
    inputStream.close();
    println("FTP Store result: " + returnResp)
    returnResp
  }

  def ftpDownload(ftpClient: FTPClient, ftpHost: String, ftpUser: String, ftpPassword: String, ftpRemoteDirectory: String, ftpFile: String): InputStream = {
    val client = new FTPClient()

    client.connect(ftpHost)

    client.login(ftpUser, ftpPassword)

    client.getReplyCode
    client.enterLocalPassiveMode
    client.changeWorkingDirectory(ftpRemoteDirectory.trim)
    val workingDirectory = client.printWorkingDirectory

    val fileNames = client.listNames
    val reqFileName = fileNames.filter(x => { x == ftpFile })(0)

    val fileContent = client.retrieveFileStream(workingDirectory + "/" + reqFileName)
    fileContent
  }

}