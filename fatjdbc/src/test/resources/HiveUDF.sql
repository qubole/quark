--  Copyright (c) 2015. Qubole Inc
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--    http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License. See accompanying LICENSE file.

create table test_hist
(
  id int,
  qbol_user_id int,
  submit_time int,
  end_time int,
  progress int,
  cube_id int,
  created_at VARCHAR ,
  updated_at VARCHAR,
  path VARCHAR,
  status VARCHAR,
  host_name VARCHAR,
  user_loc boolean,
  qbol_session_id int,
  command_id int,
  command_type VARCHAR,
  qlog VARCHAR,
  periodic_job_id int,
  wf_id VARCHAR,
  command_source VARCHAR,
  resolved_macros VARCHAR,
  status_code int,
  pid int,
  editable_pj_id int,
  template VARCHAR,
  command_template_id int,
  command_template_mutable_id int,
  can_notify boolean,
  num_result_dir int,
  start_time int,
  pool VARCHAR,
  timeout int,
  tag VARCHAR,
  name VARCHAR,
  saved_query_mutable_id int
);


create TABLE acc (
customer_name        VARCHAR,
id                   int
);

create TABLE uinfo (
qu_id                int,
u_email              VARCHAR,
a_id                 int,
a_name               VARCHAR
);


create table test_hist_partition
(
  id int,
  qbol_user_id int,
  submit_time int,
  end_time int,
  progress int,
  cube_id int,
  created_at VARCHAR,
  updated_at VARCHAR,
  path VARCHAR,
  status VARCHAR,
  host_name VARCHAR,
  user_loc boolean,
  qbol_session_id int,
  command_id int,
  command_type VARCHAR,
  qlog VARCHAR,
  periodic_job_id int,
  wf_id VARCHAR,
  command_source VARCHAR,
  resolved_macros VARCHAR,
  status_code int,
  pid int,
  editable_pj_id int,
  template VARCHAR,
  command_template_id int,
  command_template_mutable_id int,
  can_notify boolean,
  num_result_dir int,
  start_time int,
  pool VARCHAR,
  timeout int,
  tag VARCHAR,
  name VARCHAR,
  saved_query_mutable_id int
);