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

package com.qubole.quark.fatjdbc.utility;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by adeshr on 3/2/16.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogDetail {
  public final DbCredentials dbCredentials;

  @JsonCreator
  public CatalogDetail(@JsonProperty("dbCredentials") DbCredentials dbCredentials) {
    this.dbCredentials = dbCredentials;
  }

  /**
   * Store database credentials
   */
  public static class DbCredentials {
    public final String url;
    public final String username;
    public final String password;
    public final String encryptionKey;

    @JsonCreator
    DbCredentials(@JsonProperty("url") String url,
                         @JsonProperty("username") String username,
                         @JsonProperty("password") String password,
                         @JsonProperty("encrypt_key") String encrpytionKey) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.encryptionKey = encrpytionKey;
    }
  }
}
