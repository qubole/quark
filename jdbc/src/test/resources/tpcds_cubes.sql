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

create table web_returns_cube
(
    i_item_id           char(16),

    d_year              integer,
    d_qoy               integer,
    d_moy               integer,
    d_date              date,

    cd_gender           char(1),
    cd_marital_status   char(1),
    cd_education_status char(20),

    grouping__id        varchar(50),
    total_net_loss      double,
    average_net_loss    double,
    min_return_amount   double,
    max_return_amount   double,
    web_page_count      integer
);

insert into web_returns_cube(
    i_item_id,
    d_year,
    d_qoy,
    d_moy,
    d_date,
    cd_gender,
    cd_marital_status,
    cd_education_status,
    grouping__id,
    total_net_loss,
    average_net_loss,
    min_return_amount,
    max_return_amount,
    web_page_count
) values (
1, 2015, 1, 1, '2015-01-01', 'M', 'M', 'graduate', '2', 100, 100, 50, 100, 2);