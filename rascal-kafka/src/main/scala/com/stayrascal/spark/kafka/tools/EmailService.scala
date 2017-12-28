package com.stayrascal.spark.kafka.tools

import java.security.MessageDigest
import java.util
import java.util.Properties
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Authenticator, Message, PasswordAuthentication, Session, Transport}

import org.apache.commons.codec.binary.Base64
import org.stringtemplate.v4.ST

import scala.io.Source

private class Encryption {

  private val salt: String = "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  def getPassword(): String = {
    val prop = new Properties();
    prop.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    decrypt(prop.getProperty("email.key"), prop.getProperty("email.password"))
  }

  def getEmailName(): String = {
    val prop = new Properties()
    prop.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    decrypt(prop.getProperty("email.key"), prop.getProperty("email.username"))
  }

  def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (key + salt).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  def decrypt(key: String, encryptedValue: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
    new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
  }

  def encrypt(key: String, value: String): String = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
    Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
  }
}

private class MyAuthenticator extends Authenticator {
  val encryption = new Encryption()

  override def getPasswordAuthentication() = new PasswordAuthentication(encryption.getEmailName(), encryption.getPassword())
}

object EmailService {

  case class NotifyContext(email: String, title: String, winCredits: Double, totalCredits: Double)
}

class EmailService {

  import EmailService._

  def sender(context: NotifyContext): Int = sender(context.email, context.title, generateTemplate(context))

  private def sender(toEmail: String, title: String, body: String): Int = {
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("email.properties"))
    val session = Session.getDefaultInstance(props, new MyAuthenticator())

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(new Encryption().getEmailName()))
      message.setRecipients(Message.RecipientType.TO, toEmail)
      message.setSubject(title)
      message.setText(body)
      Transport.send(message)
      200
    } catch {
      case e: Exception => e.printStackTrace(); 500
    }
  }

  def generateTemplate(context: NotifyContext): String = {
    val template = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("email.stg")).getLines().mkString("\n")
    val st = new ST(template)
    st.add("name", context.email)
    st.add("credits", context.winCredits)
    st.add("total", context.totalCredits)
    st.render()
  }
}
