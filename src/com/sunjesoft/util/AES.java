
package com.sunjesoft.util;
import java.security.MessageDigest;
import java.util.Arrays;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;



public class AES {
  static String IV = "NHWM.COMAAAAAAAA";
  public static String encryptionKey = "0123456789abcdef";
  public static void main(String [] args) {
  
    if ( args.length  != 1 ) { 
      System.out.println ("ggg");
    } 

    try {
      
      System.out.println("==Java==");
      System.out.println("plain:   " + args[0] );
      
      String x = String.format ( "%-16s", args[0]);


      String cipher = encrypt(x, encryptionKey);
      System.out.print("cipher:  ");
      System.out.print("[" + cipher +  "]");
      System.out.println("");

      String decrypted = decrypt(cipher, encryptionKey);
      System.out.println("decrypt: " + decrypted);
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

  public static String encrypt(String plainText, String encryptionKey) throws Exception {
    Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding", "SunJCE");
    SecretKeySpec key = new SecretKeySpec(encryptionKey.getBytes("UTF-8"), "AES");
    cipher.init(Cipher.ENCRYPT_MODE, key,new IvParameterSpec(IV.getBytes("UTF-8")));

    byte[]  xReturn = cipher.doFinal(plainText.getBytes("UTF-8"));

    return javax.xml.bind.DatatypeConverter.printBase64Binary(xReturn); 
  }

  public static String decrypt(String cipherText, String encryptionKey) throws Exception{
    Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding", "SunJCE");
    SecretKeySpec key = new SecretKeySpec(encryptionKey.getBytes("UTF-8"), "AES");
    cipher.init(Cipher.DECRYPT_MODE, key,new IvParameterSpec(IV.getBytes("UTF-8")));
    byte [] tmp = javax.xml.bind.DatatypeConverter.parseBase64Binary( cipherText ) ;
    return new String(cipher.doFinal(tmp),"UTF-8");
  }
}
