package com.self.dataAsService
import javax.activation._
import javax.mail._
import javax.mail.internet._


object MailService {

  println( "*******Inside MailService******" )
  
  //new MailService("pratik-p@gmail.com", "subratasankar.m@gmail.com", "Test_Mail", "Testing SMTP Connection for success mail")
  
  val recipient1 = "pratik-p@gmail.com"
  val recipient2 = "subratasankar.m@gmail.com"
  val subject = "Test_Mail"
  val content = "Testing SMTP Connection for success mail"
  val host = "smtp.sendgrid.net"
  val port = "25"
  
  val user= "azure_4d318fb66f84010e81e08aaa6610fc56@azure.com"
  val pwd= "1qazXSW@"
  
 
    val properties = System.getProperties()
    properties.setProperty("mail.smtp.host", host)
    properties.setProperty("mail.smtp.port", port)
    // properties.setProperty("mail.smtp.starttls.enable", "true")
    properties.setProperty("mail.smtp.auth", "true")
    
    val session = Session.getInstance(properties, Authenticator() )
    
    // Create a default MimeMessage object.
    val message = new MimeMessage(session);
    
    // Set From: header field of the header.
    message.setFrom(new InternetAddress(user));
    
    // Set To: header field of the header.
    message.addRecipient(Message.RecipientType.TO,
                         new InternetAddress(recipient1))
						 
	message.addRecipient(Message.RecipientType.CC,
                         new InternetAddress(recipient2))
    
    // Set Subject: header field
    message.setSubject( subject )
    
    // Now set the actual message
    message.setText( content )
    
    // Send message
    val tr = session.getTransport("smtp")
    tr.connect()
    // tr.connect( host, 25, user, pwd)
    def notifyAdmin(){
    tr.sendMessage(message, message.getAllRecipients())
	println( "*******message sent******" )
  

  }
  case class Authenticator() extends javax.mail.Authenticator {
    override def getPasswordAuthentication():PasswordAuthentication = {
      return new PasswordAuthentication(user, pwd)
    }
  }
}
