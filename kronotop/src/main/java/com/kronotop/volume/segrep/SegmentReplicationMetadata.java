/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.segrep;

public class SegmentReplicationMetadata {
    /*
    1- Volume'un yeni replikasyon protokolunde standby ile herhangi bir backup ve CDC tool arasinda fark olmayacak
    2- Standby ve master arasinda tek iletisim kanali internal port olacak. FDB uzerinden bir seyler okuyup karar almak yok
       bu durum ikincil code path yarabilir.
    3- volume.admin describe <shard-kind> <shard-id> gibi bir komutla bir shard'in arkasindaki volume'u zaten describe ediyoruz
       bu komut ya gelistirilmeli ya da volume.admin describe-segment gibi bir alt komut ile segment'in pozisyonu vesaire expose
       edilmeli.
    4- segment-range okumak icin zaten komutumuz var. segment'i blob dosya olarak goruyoruz, 0-2354 range'ini okuyabiliyoruz mesela
    5- Standby, external backup vs gibi mekanizmalar segment'in kapasitesini ve cursor'un nerede oldugunu bildiklerinde segment-range ile okur klonlarlar segment'i.
    6- Klonlama esnasinda segment vacuum tarafindan silinirse ya da ici bosaltilirsa, yani kardinalitesi sifir olursa. bir hata donulebilir.
    7- SEGMENTSTALE ve SEGMENTREMOVED gibi hata kodlari olabilir. segment-range'e bu hatalar donulurse hemen okumayi birakmasi lazim
    8- Bu segmentten sonraki ilk non-stale segment'i bularak devam etmesi lazim. yine describe komutlarini kullanacaktir.
    9- Stanby kendi stale/orphan dosyalarini silmekle sorumludur
    10- Write'a acik olan segment'in cursor'unu yakaladiktan sonra CDC'den devam etmesi gerekiyor. Ilgili segment'in segment log'unu okuyup
        operasyonlari kendine yine segment range ile kopyalayabilir. segment log'da APPEND/DELETE ve hangi araliklara vuruldugu yaziyor olacak.
    11- CDC trigger'i FDB watch gibi, onun bir wrapper'i olarak calisiyor olacak. Burasi ikinci asama.
    12- Standby rolundeki host kendi replika etme muhasebesini kendi tutacak, master karismayacak. FDB uzerinde saklayacagiz elbette.
    13- External tool'lar kendileri sorumlu muhasebe tutmaktan. Master sadece sorulana cevap veren pasif bir konumda burada.
     */
}
