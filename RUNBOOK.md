# RUNBOOK Data Warehouse PT XYZ

## 1. Gambaran Project

Project ini membangun data warehouse untuk data industri pertambangan PT
XYZ. Data sumber berupa tiga file CSV yang disimpan di folder `data/`.
Data tersebut dimuat ke tabel staging, diproses menjadi tabel dimensi,
lalu digunakan untuk mengisi tabel fakta.

SQL Server digunakan sebagai database data warehouse. Proses ETL
dijalankan melalui `standalone_etl.py`, sedangkan pemeriksaan data
dilakukan dengan `validate_dw.py` dan `test_dw.py`. Metabase digunakan
untuk membuka tabel warehouse dan membuat pertanyaan atau visualisasi.

### Arsitektur

```text
Tiga file CSV di folder data/
             |
             v
     standalone_etl.py
             |
             v
 SQL Server: PTXYZ_DataWarehouse
      |          |          |
   staging      dim        fact
      |          |          |
      +---- validasi dan test
                    |
                    v
          Metabase untuk OLAP/BI
```

Layanan tambahan seperti Airflow, Redis, PostgreSQL, Jupyter, Superset,
dan Grafana juga tercantum di Docker Compose. Untuk alur yang dijelaskan
di dokumen ini, layanan yang dijalankan terlebih dahulu adalah SQL
Server. Layanan lain dijalankan jika memang diperlukan.

## 2. Kebutuhan dan Konfigurasi

Sebelum menjalankan project, pastikan Docker Desktop aktif, Python
tersedia, dan terminal berada di folder utama project.

File `.env` harus tersedia di folder utama. File ini menyimpan
konfigurasi koneksi dan kredensial lokal. Jangan menyalin password atau
token dari file tersebut ke laporan, Git, atau ZIP yang dibagikan.

Tiga file sumber yang digunakan:

- `data/dataset_alat_berat_dw.csv`
- `data/dataset_production.csv`
- `data/dataset_transaksi.csv`

Pada konfigurasi lokal, port SQL Server di komputer dipetakan ke `1434`,
sementara port di dalam container tetap `1433`. Perubahan ini dilakukan
karena port host `1433` sudah berpotensi digunakan oleh SQL Server lain.
Karena itu, skrip ETL lokal terhubung ke `localhost,1434`. SQL Server
lain yang menggunakan port `1433` tidak perlu dihentikan.

Nama database yang digunakan pada konfigurasi lokal adalah
`PTXYZ_DataWarehouse`.

## 3. Menjalankan Database dan Inisialisasi

Jalankan perintah berikut dari folder project.

Menyalakan SQL Server:

```powershell
docker compose up -d sqlserver
```

Memeriksa container:

```powershell
docker compose ps
```

Lanjutkan jika container SQL Server sudah berjalan dan status
kesehatannya menunjukkan kondisi sehat. Setelah itu, jalankan
inisialisasi database:

```powershell
docker compose run --rm db_init
```

Skrip inisialisasi menyiapkan database dan struktur yang diperlukan.
Pada pengerjaan ini, file `init-scripts/init-db.sh` juga diperbaiki line
ending-nya menjadi LF agar dapat dijalankan dengan benar di lingkungan
container.

Untuk melihat nama layanan yang tersedia di konfigurasi saat ini,
gunakan:

```powershell
docker compose config --services
```

Jangan menjalankan `docker compose down -v` atau
`docker system prune --volumes` sebagai langkah rutin. Perintah tersebut
dapat menghapus volume atau data container yang masih diperlukan.

## 4. Memasang Dependensi dan Menjalankan ETL

Pasang dependensi Python:

```powershell
python -m pip install -r requirements.txt
```

Periksa sintaks skrip ETL:

```powershell
python -m py_compile standalone_etl.py
```

Jika pemeriksaan sintaks berhasil, jalankan proses ETL:

```powershell
python standalone_etl.py
```

Secara umum, proses ETL terdiri dari tiga bagian:

1.  **Staging.** Skrip membaca tiga CSV dan memuat isinya ke tabel
    staging.
2.  **Dimensi.** Data staging diproses untuk mengisi dimensi waktu,
    site, alat berat, material, pegawai, shift, proyek, dan akun.
3.  **Fakta.** Skrip mengisi `FactEquipmentUsage`, `FactProduction`, dan
    `FactFinancialTransaction`.

Pada data yang diperiksa, setiap file CSV memiliki 1.150 baris dan
setiap tabel staging juga memiliki 1.150 baris.

Sebagian baris data alat berat tidak memiliki `site_id`. Pada
implementasi lokal, kondisi tersebut ditangani dengan ID site sintetis
negatif agar relasi dapat dipertahankan. ID tersebut merupakan penanda
hasil penanganan data yang tidak memiliki lokasi, bukan ID site asli
dari file sumber.

Pemuatan tabel fakta pada skrip lokal dilakukan dalam transaksi. Skrip
mengosongkan isi tabel fakta yang dikelola, memasukkan kembali data,
lalu memeriksa jumlah baris dan keunikan ID. Jika pemeriksaan gagal,
transaksi dibatalkan. Karena proses ini memperbarui isi tabel fakta,
pastikan database yang digunakan memang database project ini sebelum
menjalankannya.

## 5. Struktur Data Warehouse

Database memiliki tiga kelompok schema utama:

- `staging`: menyimpan data hasil pemuatan dari CSV.
- `dim`: menyimpan data dimensi untuk memberi konteks pada pengukuran.
- `fact`: menyimpan data fakta dan nilai pengukuran.

Tabel fakta yang diperiksa:

- `fact.FactEquipmentUsage`
- `fact.FactProduction`
- `fact.FactFinancialTransaction`

Tabel dimensi yang digunakan meliputi:

- `dim.DimTime`
- `dim.DimSite`
- `dim.DimEquipment`
- `dim.DimMaterial`
- `dim.DimEmployee`
- `dim.DimShift`
- `dim.DimProject`
- `dim.DimAccount`

Tabel dimensi menyimpan atribut yang menjelaskan data fakta. Sebagai
contoh, data produksi dapat dianalisis berdasarkan waktu, site,
material, pegawai, dan shift sesuai relasi yang tersedia pada skema.

## 6. Validasi dan Pengujian

Jalankan validasi data:

```powershell
python validate_dw.py
```

Skrip ini memeriksa jumlah baris staging dan fakta, keunikan ID fakta,
foreign key yang tidak memiliki pasangan pada tabel dimensi, kecocokan
tanggal fakta dengan staging, serta kecocokan total beberapa measure
antara fakta dan staging.

Setelah itu, jalankan unit test:

```powershell
python -m unittest -v test_dw.py
```

Pada pengujian terakhir, keenam test berikut berhasil:

- koneksi mengarah ke database yang diharapkan;
- tanggal pada tabel fakta cocok dengan staging;
- tidak ditemukan orphan foreign key pada relasi fakta yang diperiksa;
- jumlah measure pada fakta cocok dengan staging;
- tabel dan kolom wajib tersedia;
- jumlah baris staging dan fakta cocok serta ID fakta unik.

Test ini memeriksa kondisi data yang disebutkan di atas. Test tersebut
tidak dengan sendirinya membuktikan bahwa seluruh tipe data kolom
identik dengan semua ketentuan dokumentasi sumber. Jika penilaian
meminta pemeriksaan tipe data secara eksplisit, pemeriksaan tersebut
perlu ditambahkan.

## 7. Hasil Pemeriksaan Terakhir

Angka berikut berasal dari eksekusi validasi terakhir selama pengerjaan.
Jalankan ulang perintah pada bagian sebelumnya untuk mendapatkan hasil
terbaru di komputer yang digunakan.

Tabel Jumlah baris

---

Masing-masing CSV sumber 1.150
Masing-masing tabel staging 1.150
`fact.FactEquipmentUsage` 1.150
`fact.FactProduction` 1.150
`fact.FactFinancialTransaction` 1.150
`dim.DimTime` 830
`dim.DimSite` 31
`dim.DimEquipment` 6
`dim.DimMaterial` 5
`dim.DimEmployee` 10
`dim.DimShift` 3
`dim.DimProject` 50
`dim.DimAccount` 30

Pemeriksaan foreign key menghasilkan nol orphan untuk relasi yang diuji.
Tanggal pada tabel fakta juga cocok dengan tanggal pada staging. Total
measure pada fakta cocok dengan hasil agregasi staging untuk kolom yang
diperiksa.

Tabel fakta Measure Total

---

Equipment usage `operating_hours` 351.667
Equipment usage `downtime_hours` 16.171
Equipment usage `fuel_consumption` 1.839.169
Equipment usage `maintenance_cost` 760.686.030
Production `produced_volume` 306.639
Production `unit_cost` 337.143
Production `material_quantity` 3.346.902
Financial transaction `budgeted_cost` 119.532.547
Financial transaction `actual_cost` 127.350.159
Financial transaction `account_cost` 54.805.130

Angka tersebut adalah hasil pemeriksaan terhadap data yang tersedia di
project. Dataset sampel masing-masing berisi 1.150 baris. Jika
dokumentasi sumber menjelaskan jumlah data yang lebih besar, perbedaan
itu perlu dicatat sebagai perbedaan antara dataset yang tersedia dan
angka pada dokumentasi, bukan ditutupi dengan membuat data pengganti.

## 8. Metabase dan Analisis OLAP

Metabase pada konfigurasi lokal dibuka melalui `http://localhost:3001`.
Koneksi Metabase ke SQL Server Data Warehouse telah berhasil dilakukan
dan tabel fakta terlihat di Metabase.

Untuk menjalankan Metabase beserta database internalnya:

```powershell
docker compose up -d postgres_metabase metabase
```

Setelah masuk ke Metabase, pilih tabel fakta dan gunakan dimensi yang
tersedia untuk menyusun pertanyaan. Contoh analisis yang bisa dibuat
setelah kolom dan relasinya diperiksa:

- **Roll-up:** menjumlahkan volume produksi per site atau proyek.
- **Drill-down:** melihat rincian produksi dari tingkat proyek ke
  waktu.
- **Slice:** menampilkan data untuk satu site, proyek, atau periode.
- **Dice:** membandingkan data untuk beberapa site/proyek dan periode.
- **Pivot:** menyusun perbandingan nilai produksi berdasarkan proyek
  dan periode jika fitur tabel pivot tersedia.

Koneksi database yang berhasil belum berarti semua analisis di atas
sudah dibuat. Simpan screenshot pertanyaan atau dashboard hanya setelah
analisis benar-benar dijalankan dan hasilnya terlihat.

## 9. Kendala Selama Pengerjaan

**Port SQL Server bentrok.** Port host `1433` berpotensi sudah
digunakan. Konfigurasi lokal memakai pemetaan `1434:1433`, sehingga
skrip ETL mengakses `localhost,1434`.

**Skrip inisialisasi gagal karena line ending.** File
`init-scripts/init-db.sh` diubah ke line ending LF. Setelah perubahan
itu, perintah inisialisasi database berhasil dijalankan.

**Versi PostgreSQL untuk Metabase.** Log sebelumnya menunjukkan versi
PostgreSQL 13 tidak memenuhi kebutuhan versi Metabase yang digunakan.
Jika versi major PostgreSQL diganti, gunakan volume baru yang sesuai.
Jangan langsung membuka volume data PostgreSQL 13 menggunakan PostgreSQL 14.

**Jumlah data sampel berbeda dari dokumentasi.** Tiga file CSV yang
tersedia masing-masing berisi 1.150 baris. File yang tersedia tetap
digunakan tanpa menggantinya dengan data sintetis.

**Sebagian data alat berat tidak memiliki site.** Skrip ETL lokal
memakai ID site sintetis negatif untuk menangani nilai lokasi yang tidak
tersedia. Hal ini merupakan keputusan implementasi lokal dan perlu
dipahami saat membaca hasil analisis.

## 10. Dokumentasi Pengujian untuk Laporan

Foto dokumentasi berada di folder docs

1. Docker (docker.png)
   Menampilkan container yang digunakan untuk menjalankan SQL Server dan Metabase.

2. Proses ETL (hasil etl.png)
   Menampilkan proses pemuatan data CSV ke staging, pengisian tabel dimensi dan fakta, serta penyelesaian pipeline ETL.

3. Validasi data (hasil validasi.png)
   Menampilkan hasil pemeriksaan jumlah baris, relasi foreign key, kecocokan tanggal, dan total measure antara tabel fakta dan staging.

4. Unit test (unit test.png)
   Menampilkan hasil enam pengujian otomatis yang berhasil dijalankan menggunakan python -m unittest -v test_dw.py.

5. Analisis OLAP (OLAP.png)
   Menampilkan grafik total volume produksi berdasarkan site yang dibuat menggunakan Metabase.
