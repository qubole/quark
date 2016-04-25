/*
 * Copyright (c) 2015. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.qubole.quark.catalog.db.encryption;

import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.sql.SQLException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Encrypt/decrypt data for/from db using Mysql AES algorithm.
 */
public class AESEncrypt implements Encrypt {
  private final String key;

  public AESEncrypt(String key) {
    this.key = key;
  }

  public static SecretKeySpec generateMySQLAESKey(String key, String encoding) {
    try {

      byte[] finalKey = new byte[16];
      int i = 0;
      for (byte b : key.getBytes(encoding)) {
        finalKey[i++ % 16] ^= b;
      }
      return new SecretKeySpec(finalKey, "AES");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String convertToDatabaseColumn(String phrase) throws SQLException {
    try {
      Cipher encryptCipher = Cipher.getInstance("AES");
      encryptCipher.init(Cipher.ENCRYPT_MODE,
          generateMySQLAESKey(this.key, "UTF-8"));

      return new String(Hex.encodeHex(encryptCipher.doFinal(phrase.getBytes("UTF-8"))));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public String convertToEntityAttribute(String phrase) throws SQLException {
    try {
      Cipher decryptCipher = Cipher.getInstance("AES");
      decryptCipher.init(Cipher.DECRYPT_MODE,
          generateMySQLAESKey(this.key, "UTF-8"));

      return new String(decryptCipher.doFinal(Hex.decodeHex(phrase.toCharArray())));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
