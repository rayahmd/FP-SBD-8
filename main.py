# ===================================================================================
# APLIKASI E-COMMERCE BERBASIS KONSOL (CLI) - VERSI FINAL YANG DISEMPURNAKAN
# Dibuat untuk Final Project Sistem Basis Data.
# ===================================================================================

# --- Import Library ---
import mysql.connector
from mysql.connector import Error
import pymongo
from pymongo.mongo_client import MongoClient
from bson.objectid import ObjectId
import getpass
from tabulate import tabulate
import os
from datetime import datetime

# --- Konfigurasi ---
DB_CONFIG = {'host': '127.0.0.1', 'user': 'root', 'password': '', 'database': 'fp_sbd'}
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "fp_sbd_nosql"

# --- Fungsi Helper ---
def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_db_connection(config):
    try: return mysql.connector.connect(**config)
    except Error as e: print(f"Error koneksi MySQL: {e}"); return None

def get_mongo_client(uri, db_name):
    try:
        client = MongoClient(uri); client.admin.command('ping'); return client[db_name]
    except Exception as e: print(f"Error koneksi MongoDB: {e}"); return None

def safe_int_input(prompt):
    while True:
        try: return int(input(prompt))
        except ValueError: print("[!] Input tidak valid. Harap masukkan angka.")

def safe_float_input(prompt):
    while True:
        try: return float(input(prompt))
        except ValueError: print("[!] Input tidak valid. Harap masukkan angka.")

# ==============================================================================
# FITUR-FITUR WORKER
# ==============================================================================

def manage_categories(conn):
    clear_screen(); print("--- Manajemen Kategori Produk ---")
    try:
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            cursor.execute("SELECT category_id, name, description FROM Categories")
            print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
        print("\n1. Tambah Kategori Baru | 2. Kembali")
        if input("Pilih: ") == '1':
            name = input("Nama Kategori Baru: "); desc = input("Deskripsi Singkat: ")
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO Categories (name, description) VALUES (%s, %s)", (name, desc))
                conn.commit(); print("\n[✓] Kategori baru berhasil ditambahkan.")
    except Error as e: print(f"\n[!] Gagal: {e}")
    input("Tekan Enter untuk melanjutkan...")

def add_product(conn, worker_id):
    try:
        clear_screen(); print("--- Tambah Produk Baru ---")
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            cursor.execute("SELECT category_id, name FROM Categories"); categories = cursor.fetchall()
            if not categories: print("[!] Tidak ada kategori. Harap tambahkan dulu."); return
            print("Pilih Kategori:"); print(tabulate(categories, headers="keys", tablefmt="grid"))
            category_id = safe_int_input("Masukkan category_id: ")
        name = input("Nama Produk: "); description = input("Deskripsi: ")
        base_price = safe_float_input("Harga Dasar: "); base_stock = safe_int_input("Stok Dasar: ")
        with conn.cursor() as cursor:
            query = "INSERT INTO Products (worker_id, category_id, name, description, base_price, base_stock) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (worker_id, category_id, name, description, base_price, base_stock))
            conn.commit(); print("\n[✓] Produk baru berhasil ditambahkan!")
    except (ValueError, Error) as e: print(f"\n[!] Gagal menambahkan produk: {e}")
    finally: input("Tekan Enter untuk melanjutkan...")

def manage_shipping_methods(conn):
    """Fungsi untuk worker mengelola (tambah, lihat, ubah, hapus) metode pengiriman."""
    while True:
        clear_screen()
        print("--- Manajemen Metode Pengiriman ---")
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                cursor.execute("SELECT shipping_method_id, shipping_name, shipping_cost, estimated_del_time_text, is_active FROM Shipping_Methods")
                methods = cursor.fetchall()
                # Menambahkan teks 'Aktif'/'Tidak Aktif' agar lebih mudah dibaca
                for method in methods:
                    method['Status'] = 'Aktif' if method.pop('is_active') else 'Tidak Aktif'
                
                print(tabulate(methods, headers="keys", tablefmt="grid") if methods else "Belum ada metode pengiriman.")
            
            print("\n1. Tambah Metode Baru")
            print("2. Hapus Metode")
            print("3. Ubah Status (Aktif/Nonaktif)")
            print("4. Kembali ke Menu Worker")
            choice = input("Pilih: ")

            if choice == '1':
                name = input("Nama Metode Pengiriman (cth: JNE OKE): ")
                cost = safe_float_input("Biaya Pengiriman (cth: 15000): ")
                est_time = input("Estimasi Waktu Pengiriman (cth: 2-4 hari kerja): ")
                
                with conn.cursor() as cursor:
                    query = "INSERT INTO Shipping_Methods (shipping_name, shipping_cost, estimated_del_time_text, is_active) VALUES (%s, %s, %s, 1)"
                    cursor.execute(query, (name, cost, est_time))
                    conn.commit()
                    print("\n[✓] Metode pengiriman baru berhasil ditambahkan dan langsung diaktifkan.")

            elif choice == '2':
                method_id = safe_int_input("Masukkan ID metode yang akan dihapus: ")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM Shipping_Methods WHERE shipping_method_id = %s", (method_id,))
                    conn.commit()
                    if cursor.rowcount > 0:
                        print("\n[✓] Metode pengiriman berhasil dihapus.")
                    else:
                        print("\n[!] ID Metode tidak ditemukan.")

            elif choice == '3':
                method_id = safe_int_input("Masukkan ID metode yang statusnya akan diubah: ")
                new_status = input("Pilih status baru (1 untuk Aktif, 0 untuk Tidak Aktif): ")
                if new_status in ['0', '1']:
                    with conn.cursor() as cursor:
                        query = "UPDATE Shipping_Methods SET is_active = %s WHERE shipping_method_id = %s"
                        cursor.execute(query, (int(new_status), method_id))
                        conn.commit()
                        if cursor.rowcount > 0:
                            print("\n[✓] Status metode pengiriman berhasil diubah.")
                        else:
                            print("\n[!] ID Metode tidak ditemukan.")
                else:
                    print("\n[!] Pilihan status tidak valid. Harap masukkan 0 atau 1.")

            elif choice == '4':
                break

        except (ValueError, Error) as e:
            print(f"\n[!] Terjadi kesalahan: {e}")
        
        input("Tekan Enter untuk melanjutkan...")


def manage_variations(conn, product_id):
    while True:
        clear_screen()
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                cursor.execute("SELECT name FROM Products WHERE product_id = %s", (product_id,)); product = cursor.fetchone()
                if not product: print("Produk tidak ditemukan."); break
                print(f"--- Mengelola Variasi untuk: {product['name']} ---")
                cursor.execute("SELECT variation_id, variation_name, stock_quantity, price_override FROM Product_Variations WHERE product_id = %s", (product_id,))
                print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid") or "Belum ada variasi.")
            print("\n1. Tambah Variasi Baru | 2. Kembali")
            choice = input("Pilih: ")
            if choice == '1':
                var_name = input("Nama Variasi: "); var_sku = input("SKU Variasi: "); var_stock = safe_int_input("Jumlah Stok: ")
                price_override_str = input("Harga Spesifik (kosongkan jika sama): "); price_override = float(price_override_str) if price_override_str else None
                with conn.cursor() as cursor:
                    query = "INSERT INTO Product_Variations (product_id, variation_name, variation_sku, stock_quantity, price_override) VALUES (%s, %s, %s, %s, %s)"
                    cursor.execute(query, (product_id, var_name, var_sku, var_stock, price_override)); conn.commit()
                    print("\n[✓] Variasi baru berhasil ditambahkan.")
            elif choice == '2': break
        except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
        input("Tekan Enter untuk melanjutkan...")

def manage_products(conn, worker_id):
    while True:
        clear_screen(); print("--- Manajemen Produk ---")
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            query = "SELECT p.product_id, p.name, c.name as category, p.base_price, p.base_stock FROM Products p LEFT JOIN Categories c ON p.category_id = c.category_id ORDER BY p.product_id;"
            cursor.execute(query); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
        print("\n1. Tambah Produk Baru | 2. Kelola Variasi Produk | 3. Kembali")
        choice = input("Pilih: ")
        if choice == '1': add_product(conn, worker_id)
        elif choice == '2': manage_variations(conn, safe_int_input("Masukkan product_id untuk mengelola variasinya: "))
        elif choice == '3': break

def add_promotion(conn):
    try:
        clear_screen(); print("--- Tambah Promosi Baru ---")
        code = input("Kode Promosi (unik): ").upper(); description = input("Deskripsi: ")
        while True:
            discount_type = input("Tipe Diskon (PERCENTAGE/FIXED_AMOUNT): ").upper()
            if discount_type in ["PERCENTAGE", "FIXED_AMOUNT"]: break
            else: print("[!] Tipe tidak valid.")
        discount_value = safe_float_input("Nilai Diskon: "); min_purchase = safe_float_input("Minimum Pembelian: ")
        start_date = input("Tanggal Mulai (YYYY-MM-DD HH:MM:SS): "); end_date = input("Tanggal Berakhir (YYYY-MM-DD HH:MM:SS): ")
        with conn.cursor() as cursor:
            query = "INSERT INTO Promotions (code, description, discount_type, discount_value, min_purchase_amount, start_date, end_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (code, description, discount_type, discount_value, min_purchase, start_date, end_date)); conn.commit()
            print("\n[✓] Promosi baru berhasil ditambahkan!")
    except (ValueError, Error) as e: print(f"\n[!] Gagal menambahkan promosi: {e}")
    finally: input("Tekan Enter...")

def link_promotion_to(conn, link_type):
    clear_screen(); print(f"--- Tautkan Promosi ke {link_type.capitalize()} ---")
    try:
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            cursor.execute("SELECT promotion_id, code, description FROM Promotions"); promotions = cursor.fetchall()
            if not promotions: print("\n[!] Tidak ada promosi."); return
            print(tabulate(promotions, headers="keys", tablefmt="grid")); promo_id = safe_int_input("\nMasukkan promotion_id: ")
            if link_type == "product":
                cursor.execute("SELECT product_id, name FROM Products"); link_items = cursor.fetchall()
                id_column, name_column, table_name = "product_id", "Produk", "promotion_product"
            else:
                cursor.execute("SELECT category_id, name FROM Categories"); link_items = cursor.fetchall()
                id_column, name_column, table_name = "category_id", "Kategori", "promotion_category"
            if not link_items: print(f"\n[!] Tidak ada {link_type} untuk ditautkan."); return
            print(f"\n--- Daftar {name_column} ---"); print(tabulate(link_items, headers="keys", tablefmt="grid"))
            link_id = safe_int_input(f"\nMasukkan {id_column} tujuan: ")
            query = f"INSERT INTO {table_name} (promotion_id, {id_column}) VALUES (%s, %s)"
            cursor.execute(query, (promo_id, link_id)); conn.commit()
            print(f"\n[✓] Promosi berhasil ditautkan ke {link_type}!")
    except Error as e:
        if e.errno == 1062: print(f"\n[!] Gagal: Promosi ini sudah ditautkan ke {link_type} tersebut.")
        else: print(f"\n[!] Gagal menautkan: {e}")
    except (ValueError): print("\n[!] Input ID tidak valid.")
    finally: input("Tekan Enter...")

def manage_promotions(conn):
    while True:
        clear_screen(); print("--- Manajemen Promosi ---"); print("1. Lihat Semua Promosi"); print("2. Tambah Promosi Baru"); print("3. Tautkan Promosi ke Produk"); print("4. Tautkan Promosi ke Kategori"); print("5. Kembali")
        choice = input("Pilih: ")
        if choice == '1':
            with conn.cursor(buffered=True, dictionary=True) as cursor: cursor.execute("SELECT * FROM Promotions"); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
            input("\nTekan Enter...")
        elif choice == '2': add_promotion(conn)
        elif choice == '3': link_promotion_to(conn, "product")
        elif choice == '4': link_promotion_to(conn, "category")
        elif choice == '5': break

def manage_orders(conn):
    while True:
        clear_screen()
        print("--- Manajemen Pesanan Masuk ---")
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                # PERBAIKAN: Mengubah c.consumer_id menjadi c.Consumer_ID sesuai skema SQL
                query = """
                    SELECT 
                        o.order_id, 
                        c.name AS customer, 
                        o.order_date, 
                        o.status, 
                        o.grand_total 
                    FROM `order` o 
                    JOIN consumer c ON o.consumer_id = c.Consumer_ID 
                    ORDER BY o.order_date DESC 
                    LIMIT 15
                """
                cursor.execute(query)
                orders = cursor.fetchall()
                
                if not orders:
                    print("Belum ada pesanan.")
                    input("\nTekan Enter untuk kembali...")
                    break

                print(tabulate(orders, headers="keys", tablefmt="grid"))
                print("\n1. Update Status Pesanan | 2. Kembali")
                choice = input("Pilih: ")

                if choice == '1':
                    order_id = safe_int_input("Masukkan order_id: ")
                    if not any(o['order_id'] == order_id for o in orders):
                        print("[!] ID Pesanan tidak valid.")
                        input("Tekan Enter untuk melanjutkan...")
                        continue

                    print("Status Baru: a. Shipped | b. Completed | c. Cancelled")
                    status_map = {'a': 'Shipped', 'b': 'Completed', 'c': 'Cancelled'}
                    new_status_choice = input("Pilih (a/b/c): ").lower()
                    new_status = status_map.get(new_status_choice)
                    
                    if new_status:
                        # Gunakan cursor baru yang terisolasi untuk operasi UPDATE
                        with conn.cursor() as update_cursor:
                            update_cursor.execute("UPDATE `order` SET status = %s WHERE order_id = %s", (new_status, order_id))
                        conn.commit()
                        print(f"\n[✓] Status pesanan #{order_id} berhasil diubah.")
                    else:
                        print("[!] Pilihan status tidak valid.")
                
                elif choice == '2':
                    break
        
        except Error as e:
            print(f"\n[!] Terjadi kesalahan saat mengakses database: {e}")
            input("\nTekan Enter untuk melanjutkan...")
            break

        input("Tekan Enter untuk melanjutkan...")

def manage_reviews(conn, mongo_db):
    clear_screen(); print("--- Manajemen Ulasan Pelanggan ---")
    reviews_collection = mongo_db["reviews"]; all_reviews = list(reviews_collection.find())
    if not all_reviews: print("Belum ada ulasan."); input("\nTekan Enter..."); return
    product_ids = list(set([review.get('id_produk_sql') for review in all_reviews if review.get('id_produk_sql') is not None]))
    products = {}
    if product_ids:
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            format_strings = ','.join(['%s'] * len(product_ids))
            cursor.execute(f"SELECT product_id, name FROM Products WHERE product_id IN ({format_strings})", tuple(product_ids))
            products = {p['product_id']: p['name'] for p in cursor.fetchall()}
    display_data = [{'ID Ulasan': r['_id'], 'Produk': products.get(r.get('id_produk_sql'), 'N/A'), 'Rating': r.get('rating'), 'Komentar': r.get('komentar')} for r in all_reviews]
    print(tabulate(display_data, headers="keys", tablefmt="grid")); print("\n1. Hapus Ulasan | 2. Kembali")
    if input("Pilih: ") == '1':
        try:
            review_id = input("Masukkan ID Ulasan yang akan dihapus: ")
            result = reviews_collection.delete_one({"_id": ObjectId(review_id)})
            if result.deleted_count > 0: print("\n[✓] Ulasan berhasil dihapus.")
            else: print("\n[!] ID Ulasan tidak ditemukan.")
        except Exception as e: print(f"ID tidak valid. Error: {e}")
    input("Tekan Enter...")

def view_reports(conn):
    while True:
        clear_screen(); print("--- Menu Laporan Analitik ---"); print("1. Pendapatan per Bulan"); print("2. Pelanggan Terbaik"); print("3. Penjualan per Kategori"); print("4. Kembali")
        choice = input("Pilih: ")
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                if choice == '1':
                    year_month = input("Masukkan periode (YYYY-MM): ")
                    query = "SELECT DATE_FORMAT(order_date, '%Y-%m') as periode, COUNT(order_id) as jumlah_pesanan, SUM(grand_total) as total_pendapatan FROM `order` WHERE DATE_FORMAT(order_date, '%Y-%m') = %s GROUP BY periode;"
                    cursor.execute(query, (year_month,)); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
                
                elif choice == '2':
                    # PERBAIKAN: Menggunakan c.Consumer_ID sesuai skema SQL
                    query = "SELECT c.name as nama_pelanggan, COUNT(o.order_id) as jumlah_transaksi, SUM(o.grand_total) as total_belanja FROM `order` o JOIN consumer c ON o.consumer_id = c.Consumer_ID GROUP BY o.consumer_id ORDER BY total_belanja DESC LIMIT 10;"
                    cursor.execute(query); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
                
                elif choice == '3':
                    query = "SELECT c.name as category, COUNT(oi.order_item_id) as items_sold, SUM(oi.total_price_for_item) as category_revenue FROM order_item oi JOIN products p ON oi.product_id = p.product_id JOIN categories c ON p.category_id = c.category_id GROUP BY c.category_id ORDER BY category_revenue DESC;"
                    cursor.execute(query); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
                
                elif choice == '4': break
        
        except (ValueError, Error) as e: print(f"\n[!] Error: {e}")
        input("\nTekan Enter...")

def show_worker_menu(conn, mongo_db, user_data):
    while True:
        clear_screen()
        print(f"--- Menu Worker ({user_data['name']}) ---")
        print("1. Manajemen Kategori")
        print("2. Manajemen Produk")
        print("3. Manajemen Promosi")
        print("4. Manajemen Pesanan")
        print("5. Manajemen Ulasan")
        print("6. Lihat Laporan")
        print("7. Manajemen Metode Pengiriman") # <-- Teks diubah sesuai fungsi baru
        print("8. Logout")

        choice = input("Pilih: ")
        if choice == '1':
            manage_categories(conn)
        elif choice == '2':
            manage_products(conn, user_data['worker_id'])
        elif choice == '3':
            manage_promotions(conn)
        elif choice == '4':
            manage_orders(conn)
        elif choice == '5':
            manage_reviews(conn, mongo_db)
        elif choice == '6':
            view_reports(conn)
        elif choice == '7':
            manage_shipping_methods(conn) # <-- Memanggil fungsi manajemen yang benar
        elif choice == '8':
            break

# ==============================================================================
# FITUR-FITUR CONSUMER
# ==============================================================================

def manage_addresses(conn, consumer_id):
    while True:
        clear_screen(); print("--- Kelola Alamat Pengiriman ---")
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                # PERBAIKAN: Menggunakan Shipping_Address_ID sesuai skema SQL
                cursor.execute("SELECT Shipping_Address_ID, recipient_name, address_line1, city, is_default_shipping FROM shipping_address WHERE consumer_id = %s", (consumer_id,))
                addresses = cursor.fetchall(); print(tabulate(addresses, headers="keys", tablefmt="grid") if addresses else "Anda belum memiliki alamat.")
            
            print("\n1. Tambah Alamat Baru | 2. Atur Alamat Default | 3. Hapus Alamat | 4. Kembali")
            choice = input("Pilih: ")
            
            if choice == '1':
                r = input("Nama Penerima: "); p = input("No. Telepon: "); a1 = input("Alamat: "); c = input("Kota: "); pv = input("Provinsi: "); pc = input("Kode Pos: ")
                with conn.cursor(buffered=True) as cursor: # Tambah buffered=True
                    q = "INSERT INTO shipping_address (consumer_id, recipient_name, phone_number, address_line1, city, province, postal_code, country) VALUES (%s,%s,%s,%s,%s,%s,%s, 'Indonesia')"
                    cursor.execute(q, (consumer_id, r, p, a1, c, pv, pc)); conn.commit(); print("\n[✓] Alamat baru ditambahkan.")
            
            elif choice == '2':
                addr_id = safe_int_input("Masukkan ID alamat untuk dijadikan default: ")
                with conn.cursor(buffered=True) as cursor: # Tambah buffered=True
                    # PERBAIKAN: Menggunakan nama kolom yang benar di WHERE clause
                    cursor.execute("UPDATE shipping_address SET is_default_shipping = 0 WHERE consumer_id = %s", (consumer_id,))
                    cursor.execute("UPDATE shipping_address SET is_default_shipping = 1 WHERE Shipping_Address_ID = %s AND consumer_id = %s", (addr_id, consumer_id))
                    cursor.execute("UPDATE consumer SET shipping_address_id = %s WHERE Consumer_ID = %s", (addr_id, consumer_id)); conn.commit()
                    print("\n[✓] Alamat default berhasil diubah.")
            
            elif choice == '3':
                addr_id = safe_int_input("Masukkan ID alamat yang akan dihapus: ")
                with conn.cursor(buffered=True) as cursor: # Tambah buffered=True
                    # PERBAIKAN: Menggunakan nama kolom yang benar di WHERE clause
                    cursor.execute("DELETE FROM shipping_address WHERE Shipping_Address_ID = %s AND consumer_id = %s", (addr_id, consumer_id)); conn.commit()
                    if cursor.rowcount > 0: print("\n[✓] Alamat berhasil dihapus.")
                    else: print("\n[!] Alamat tidak ditemukan.")
            
            elif choice == '4': break
        except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
        input("Tekan Enter...")

def manage_payment_methods(conn, consumer_id):
    while True:
        clear_screen()
        print("--- Kelola Metode Pembayaran ---")
        try:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                # PERBAIKAN: Menggunakan nama kolom yang benar (Payment_method_id, Consumer_ID)
                query = """
                    SELECT 
                        pm.Payment_method_id, 
                        pm.method_type, 
                        pm.details,
                        CASE 
                            WHEN c.payment_method_id = pm.Payment_method_id THEN 'Ya'
                            ELSE 'Tidak'
                        END AS is_default
                    FROM payment_methods pm
                    JOIN consumer c ON pm.consumer_id = c.Consumer_ID
                    WHERE pm.consumer_id = %s
                """
                cursor.execute(query, (consumer_id,))
                methods = cursor.fetchall()
                if methods:
                    for m in methods: m['Default'] = m.pop('is_default')
                print(tabulate(methods, headers="keys", tablefmt="grid") if methods else "Anda belum memiliki metode pembayaran.")
            
            print("\n1. Tambah Metode Baru | 2. Atur Metode Default | 3. Hapus Metode | 4. Kembali")
            choice = input("Pilih: ")

            if choice == '1':
                m_type = input(f"Tipe Metode (cth: GoPay): "); details = input(f"Detail (cth: Nomor Telepon): ")
                with conn.cursor(buffered=True) as cursor: # Tambah buffered=True
                    q = "INSERT INTO payment_methods (consumer_id, method_type, details) VALUES (%s, %s, %s)"
                    cursor.execute(q, (consumer_id, m_type, details)); conn.commit()
                    print("\n[✓] Metode pembayaran baru berhasil ditambahkan.")

            elif choice == '2':
                method_id = safe_int_input("Masukkan ID metode untuk dijadikan default: ")
                with conn.cursor(buffered=True) as cursor: # Tambah buffered=True
                    # PERBAIKAN: Menggunakan Consumer_ID
                    q = "UPDATE consumer SET payment_method_id = %s WHERE Consumer_ID = %s"
                    cursor.execute(q, (method_id, consumer_id)); conn.commit()
                    if cursor.rowcount > 0: print("\n[✓] Metode pembayaran default berhasil diubah.")
                    else: print("\n[!] Gagal mengubah default. Pastikan ID metode benar.")

            elif choice == '3':
                method_id_to_delete = safe_int_input("Masukkan ID metode yang akan dihapus: ")
                conn.start_transaction()
                try:
                    with conn.cursor(buffered=True, dictionary=True) as cursor:
                        # PERBAIKAN: Menggunakan Consumer_ID
                        cursor.execute("SELECT payment_method_id FROM consumer WHERE Consumer_ID = %s", (consumer_id,))
                        current_default = cursor.fetchone()
                        
                        if current_default and current_default['payment_method_id'] == method_id_to_delete:
                            cursor.execute("UPDATE consumer SET payment_method_id = NULL WHERE Consumer_ID = %s", (consumer_id,))

                        # PERBAIKAN: Menggunakan Payment_method_id
                        cursor.execute("DELETE FROM payment_methods WHERE Payment_method_id = %s AND consumer_id = %s", (method_id_to_delete, consumer_id))
                        
                        if cursor.rowcount > 0: conn.commit(); print("\n[✓] Metode pembayaran berhasil dihapus.")
                        else: conn.rollback(); print("\n[!] Metode tidak ditemukan.")
                except Error as e_trans: conn.rollback(); print(f"\n[!] Gagal menghapus: {e_trans}")
            
            elif choice == '4': break
        except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
        input("Tekan Enter untuk melanjutkan...")

def manage_profile(conn, consumer_id):
    while True:
        clear_screen()
        print("--- Manajemen Profil Saya ---")
        print("1. Kelola Alamat Pengiriman")
        print("2. Kelola Metode Pembayaran")
        print("3. Kembali")
        choice = input("Pilih: ")
        if choice == '1':
            manage_addresses(conn, consumer_id)
        elif choice == '2':
            manage_payment_methods(conn, consumer_id)
        elif choice == '3':
            break

def view_products_consumer(conn, mongo_db):
    clear_screen(); print("--- Daftar Produk ---")
    with conn.cursor(buffered=True, dictionary=True) as cursor:
        cursor.execute("SELECT product_id, name, description, base_price FROM Products")
        products = cursor.fetchall(); print(tabulate(products, headers="keys", tablefmt="grid"))
        try:
            prod_id_choice = safe_int_input("\nMasukkan ID produk untuk lihat detail (atau 0 untuk kembali): ")
            if prod_id_choice > 0:
                cursor.execute("SELECT variation_id, variation_name, stock_quantity, price_override FROM Product_Variations WHERE product_id = %s AND stock_quantity > 0", (prod_id_choice,))
                variations = cursor.fetchall(); print("\n--- Variasi Tersedia ---"); print(tabulate(variations, headers="keys", tablefmt="grid") or "Stok variasi habis.")
                reviews_collection = mongo_db["reviews"]; reviews = list(reviews_collection.find({"id_produk_sql": prod_id_choice}, {"_id":0, "rating":1, "komentar":1}))
                print("\n--- Ulasan Pelanggan ---"); print(tabulate(reviews, headers="keys", tablefmt="grid") if reviews else "Belum ada ulasan.")
        except (ValueError, Error) as e: print(f"Error: {e}")
    input("\nTekan Enter untuk kembali...")

def add_to_cart(conn, mongo_db, consumer_id):
    """Menampilkan produk terlebih dahulu untuk memudahkan proses penambahan ke keranjang."""
    carts_collection = mongo_db["carts"]
    
    try:
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            # Langkah 1: Tampilkan semua produk yang dijual
            clear_screen()
            print("--- Pilih Produk untuk Ditambahkan ke Keranjang ---")
            cursor.execute("SELECT product_id, name, description, base_price FROM Products")
            products = cursor.fetchall()
            
            if not products:
                print("Saat ini tidak ada produk yang dijual.")
                input("\nTekan Enter untuk kembali...")
                return

            print(tabulate(products, headers="keys", tablefmt="grid"))

            # Langkah 2: Minta pengguna memilih produk dari daftar di atas
            product_id = safe_int_input("\nMasukkan ID produk yang ingin ditambahkan (atau 0 untuk kembali): ")

            if product_id == 0:
                return

            # Validasi apakah produk ID yang dimasukkan ada di daftar
            chosen_product = next((p for p in products if p['product_id'] == product_id), None)
            if not chosen_product:
                print("\n[!] ID Produk tidak valid.")
                input("Tekan Enter...")
                return

            # Langkah 3: Tampilkan variasi HANYA untuk produk yang dipilih
            clear_screen()
            print(f"--- Memilih Variasi untuk: {chosen_product['name']} ---")
            cursor.execute(
                "SELECT variation_id, variation_name, stock_quantity, price_override FROM Product_Variations WHERE product_id = %s AND stock_quantity > 0",
                (product_id,)
            )
            variations = cursor.fetchall()

            if not variations:
                print("Maaf, semua variasi untuk produk ini sedang habis.")
                input("\nTekan Enter untuk kembali...")
                return

            print("Variasi yang Tersedia:")
            print(tabulate(variations, headers="keys", tablefmt="grid"))
            
            variation_id = safe_int_input("\nMasukkan ID variasi yang diinginkan: ")

            # Validasi apakah variasi ID yang dimasukkan ada di daftar
            chosen_variation = next((v for v in variations if v['variation_id'] == variation_id), None)
            if not chosen_variation:
                print("\n[!] ID Variasi tidak valid.")
                input("Tekan Enter...")
                return
            
            quantity = safe_int_input(f"Jumlah '{chosen_variation['variation_name']}' yang ingin dibeli: ")

            if quantity <= 0:
                print("\n[!] Jumlah harus lebih dari 0.")
                input("Tekan Enter...")
                return

            # Langkah 4: Proses penambahan ke keranjang (logika inti yang sudah ada)
            # Kita menggunakan data yang sudah kita ambil untuk efisiensi
            if quantity > chosen_variation['stock_quantity']:
                raise ValueError(f"Stok tidak mencukupi. Sisa stok: {chosen_variation['stock_quantity']}")
                
            price = float(chosen_variation['price_override'] if chosen_variation['price_override'] else chosen_product['base_price'])
            
            cart_item = {
                "product_id": product_id,
                "variation_id": variation_id,
                "product_name": chosen_product['name'],
                "variation_name": chosen_variation['variation_name'],
                "quantity": quantity,
                "price_at_cart": price
            }
            
            # Logika untuk menambahkan atau memperbarui item di keranjang MongoDB
            # Cek apakah item dengan variasi yang sama sudah ada di keranjang
            existing_item = carts_collection.find_one(
                {"consumer_id": consumer_id, "items.variation_id": variation_id}
            )

            if existing_item:
                # Jika sudah ada, update kuantitasnya ($inc)
                carts_collection.update_one(
                    {"consumer_id": consumer_id, "items.variation_id": variation_id},
                    {"$inc": {"items.$.quantity": quantity}, "$set": {"last_updated": datetime.now()}}
                )
                print(f"\n[✓] Kuantitas untuk produk ini di keranjang berhasil diperbarui.")
            else:
                # Jika belum ada, tambahkan sebagai item baru ($push)
                carts_collection.update_one(
                    {"consumer_id": consumer_id},
                    {"$push": {"items": cart_item}, "$set": {"last_updated": datetime.now()}},
                    upsert=True
                )
                print("\n[✓] Produk berhasil ditambahkan ke keranjang!")

    except (ValueError, Error) as e:
        print(f"\n[!] Gagal menambahkan ke keranjang: {e}")
    
    input("Tekan Enter untuk melanjutkan...")

def view_cart(conn, mongo_db, consumer_id):
    """Menampilkan isi keranjang belanja konsumen dan menyediakan opsi manajemen."""
    carts_collection = mongo_db["carts"]
    
    while True:
        clear_screen()
        print("--- Keranjang Belanja Anda ---")
        
        cart = carts_collection.find_one({"consumer_id": consumer_id})

        if not cart or not cart.get("items"):
            print("Keranjang Anda kosong.")
            input("\nTekan Enter untuk kembali...")
            return

        # Menambahkan nomor urut untuk memudahkan pengguna memilih item
        cart_items_display = []
        for i, item in enumerate(cart['items'], 1):
            item_display = item.copy()
            item_display['No.'] = i
            # Memindahkan 'No.' ke depan untuk tampilan yang lebih baik
            item_display = {'No.': item_display.pop('No.'), **item_display}
            cart_items_display.append(item_display)

        # Menghapus kolom ID yang tidak perlu ditampilkan ke pengguna
        for item in cart_items_display:
            item.pop('product_id', None)
            item.pop('variation_id', None)

        print(tabulate(cart_items_display, headers="keys", tablefmt="grid"))
        
        subtotal = sum(item['price_at_cart'] * item['quantity'] for item in cart['items'])
        print(f"\nSubtotal: Rp {subtotal:,.2f}")

        print("\n1. Ubah Kuantitas Item")
        print("2. Hapus Item dari Keranjang")
        print("3. Kembali ke Menu Utama")
        choice = input("Pilih: ")

        if choice == '1':
            try:
                item_num = safe_int_input("Masukkan nomor item yang kuantitasnya ingin diubah: ")
                if not 1 <= item_num <= len(cart['items']):
                    print("[!] Nomor item tidak valid.")
                    input("Tekan Enter...")
                    continue

                item_to_update = cart['items'][item_num - 1]
                new_quantity = safe_int_input(f"Masukkan kuantitas baru untuk '{item_to_update['product_name']}': ")
                
                if new_quantity <= 0:
                    print("[!] Kuantitas harus lebih dari 0. Untuk menghapus, gunakan menu 'Hapus Item'.")
                    input("Tekan Enter...")
                    continue

                # Pengecekan stok real-time ke database SQL
                with conn.cursor(buffered=True, dictionary=True) as cursor:
                    cursor.execute("SELECT stock_quantity FROM Product_Variations WHERE variation_id = %s", (item_to_update['variation_id'],))
                    stock_data = cursor.fetchone()
                    if new_quantity > stock_data['stock_quantity']:
                        print(f"[!] Stok tidak mencukupi. Sisa stok: {stock_data['stock_quantity']}")
                    else:
                        # Update kuantitas di MongoDB menggunakan positional operator ($)
                        carts_collection.update_one(
                            {"consumer_id": consumer_id, "items.variation_id": item_to_update['variation_id']},
                            {"$set": {"items.$.quantity": new_quantity}}
                        )
                        print("[✓] Kuantitas berhasil diperbarui.")

            except (ValueError, Error) as e:
                print(f"[!] Terjadi kesalahan: {e}")
            input("Tekan Enter...")

        elif choice == '2':
            try:
                item_num = safe_int_input("Masukkan nomor item yang ingin dihapus dari keranjang: ")
                if not 1 <= item_num <= len(cart['items']):
                    print("[!] Nomor item tidak valid.")
                    input("Tekan Enter...")
                    continue
                
                item_to_remove = cart['items'][item_num - 1]
                
                # Hapus item dari array di MongoDB menggunakan $pull
                carts_collection.update_one(
                    {"consumer_id": consumer_id},
                    {"$pull": {"items": {"variation_id": item_to_remove['variation_id']}}}
                )
                print(f"[✓] Item '{item_to_remove['product_name']}' berhasil dihapus dari keranjang.")

            except ValueError as e:
                print(f"[!] Terjadi kesalahan: {e}")
            input("Tekan Enter...")

        elif choice == '3':
            break

def checkout(conn, mongo_db, consumer_id):
    carts_collection = mongo_db["carts"]
    cart = carts_collection.find_one({"consumer_id": consumer_id})
    if not cart or not cart.get("items"):
        print("Keranjang kosong.")
        input("\nTekan Enter...")
        return

    try:
        # Selalu amankan dengan rollback di awal untuk membersihkan state dari fungsi LAIN.
        conn.rollback() 
        
        # Fase 1: Cek ketersediaan metode pengiriman dalam blok cursor terisolasi.
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            cursor.execute("SELECT COUNT(*) as method_count FROM Shipping_Methods WHERE is_active = 1")
            result = cursor.fetchone()
        if result['method_count'] == 0:
            clear_screen()
            print("[!] Maaf, saat ini tidak ada metode pengiriman yang tersedia.")
            print("[!] Checkout tidak dapat dilanjutkan. Silakan hubungi administrator.")
            input("\nTekan Enter untuk kembali...")
            return

        clear_screen()
        print("--- Checkout ---")
        print(tabulate(cart['items'], headers="keys", tablefmt="grid"))
        subtotal = sum(item['price_at_cart'] * item['quantity'] for item in cart['items'])
        print(f"\nSubtotal: Rp {subtotal:,.2f}")
        
        discount_amount = 0
        promotion_id = None
        promo_code = input("Masukkan kode promosi (atau tekan Enter untuk melewati): ").upper()
        
        # Fase 2: Cek kode promosi dalam blok cursor terisolasi.
        if promo_code:
            with conn.cursor(buffered=True, dictionary=True) as cursor:
                cursor.execute("SELECT * FROM Promotions WHERE code = %s AND is_active = 1 AND NOW() BETWEEN start_date AND end_date", (promo_code,))
                promo = cursor.fetchone()
            
            if promo and subtotal >= float(promo['min_purchase_amount']):
                promotion_id = promo['promotion_id']
                if promo['discount_type'] == 'PERCENTAGE':
                    discount_amount = (subtotal * float(promo['discount_value']) / 100)
                else:
                    discount_amount = float(promo['discount_value'])
                print(f"[✓] Promosi '{promo_code}' diterapkan! Diskon: - Rp {discount_amount:,.2f}")
            else:
                print(f"[!] Kode promosi '{promo_code}' tidak valid atau tidak memenuhi syarat.")
        
        # Fase 3: Ambil dan tampilkan metode pengiriman dalam blok cursor terisolasi.
        print("\n--- Pilih Metode Pengiriman ---")
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            cursor.execute("SELECT shipping_method_id, shipping_name, shipping_cost FROM Shipping_Methods WHERE is_active = 1")
            shipping_methods = cursor.fetchall()
        print(tabulate(shipping_methods, headers="keys", tablefmt="grid"))
        
        chosen_shipping_id = safe_int_input("Masukkan ID pengiriman: ")
        chosen_method = next((m for m in shipping_methods if m['shipping_method_id'] == chosen_shipping_id), None)
        if not chosen_method:
            print("[!] ID pengiriman tidak valid."); input("\nTekan Enter..."); return
            
        shipping_cost = float(chosen_method['shipping_cost'])
        grand_total = (subtotal - discount_amount) + shipping_cost
        print(f"\nBiaya Pengiriman: Rp {shipping_cost:,.2f}"); print(f"Grand Total: Rp {grand_total:,.2f}")

        if input("Lanjutkan pembayaran (y/n)? ").lower() != 'y': return

        # Fase 4: Transaksi utama (semua INSERT/UPDATE) dalam satu blok cursor terisolasi.
        conn.start_transaction()
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            query_snapshot = """
                SELECT s.address_line1, s.city, s.province, p.method_type, p.details
                FROM Consumer c 
                LEFT JOIN Shipping_Address s ON c.shipping_address_id = s.Shipping_Address_ID 
                LEFT JOIN Payment_Methods p ON c.payment_method_id = p.Payment_method_id 
                WHERE c.Consumer_ID = %s
            """
            cursor.execute(query_snapshot, (consumer_id,))
            snapshot_data = cursor.fetchone()

            shipping_snapshot = f"{snapshot_data['address_line1']}, {snapshot_data['city']}" if snapshot_data and snapshot_data['address_line1'] else "Alamat default tidak diatur"
            payment_snapshot = f"{snapshot_data['method_type']} ({snapshot_data['details']})" if snapshot_data and snapshot_data['method_type'] else "Metode pembayaran default tidak diatur"

            order_query = "INSERT INTO `Order` (consumer_id, shipping_method_id, promotion_id, order_date, status, grand_total, subtotal_amount, discount_amount, shipping_costs_charged, shipping_address_snapshot, payment_method_snapshot) VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(order_query, (consumer_id, chosen_shipping_id, promotion_id, 'Processing', grand_total, subtotal, discount_amount, shipping_cost, shipping_snapshot, payment_snapshot))
            order_id = cursor.lastrowid

            for item in cart['items']:
                stock_query = "UPDATE Product_Variations SET stock_quantity = stock_quantity - %s WHERE variation_id = %s AND stock_quantity >= %s"
                cursor.execute(stock_query, (item['quantity'], item['variation_id'], item['quantity']))
                if cursor.rowcount == 0: raise Exception(f"Stok untuk '{item['product_name']}' habis!")
                
                item_query = "INSERT INTO Order_Item (order_id, product_id, variation_id, quantity, price_per_unit_snapshot, product_name_snapshot, variation_name_snapshot, total_price_for_item) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(item_query, (order_id, item['product_id'], item['variation_id'], item['quantity'], item['price_at_cart'], item['product_name'], item['variation_name'], item['price_at_cart'] * item['quantity']))
        
        conn.commit()
        carts_collection.delete_one({"consumer_id": consumer_id})
        print("\n[✓] Checkout berhasil!")
        
    except (ValueError, Error, Exception) as e:
        print(f"\n[!] Checkout GAGAL: {e}")
        if conn and conn.is_connected():
            try: conn.rollback()
            except Error as rb_error: print(f"[!] Gagal melakukan rollback: {rb_error}")
    # 'finally' tidak lagi diperlukan karena 'with' sudah menangani penutupan cursor secara otomatis.
            
    input("\nTekan Enter untuk melanjutkan...")

def view_purchase_history(conn, consumer_id):
    clear_screen(); print("--- Riwayat Pembelian Anda ---"); cursor = None
    try:
        cursor = conn.cursor(buffered=True, dictionary=True)
        cursor.execute("SELECT order_id, order_date, status, grand_total FROM `Order` WHERE consumer_id = %s ORDER BY order_date DESC", (consumer_id,)); orders = cursor.fetchall()
        if not orders: print("Anda belum pernah membeli."); input("\nTekan Enter..."); return
        print(tabulate(orders, headers="keys", tablefmt="grid"))
        order_id_choice = safe_int_input("\nMasukkan 'order_id' untuk lihat detail (0 untuk kembali): ")
        if order_id_choice > 0 and any(o['order_id'] == order_id_choice for o in orders):
            item_query = "SELECT product_name_snapshot as produk, variation_name_snapshot as variasi, quantity as qty, price_per_unit_snapshot as harga_satuan, total_price_for_item as total FROM Order_Item WHERE order_id = %s"
            cursor.execute(item_query, (order_id_choice,)); items = cursor.fetchall()
            clear_screen(); print(f"--- Detail Pesanan #{order_id_choice} ---"); print(tabulate(items, headers="keys", tablefmt="grid"))
    except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
    finally:
        if cursor: cursor.close()
    input("\nTekan Enter...")

def add_review(conn, mongo_db, consumer_id):
    clear_screen()
    print("--- Beri Ulasan Produk ---")
    try:
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            # --- QUERY YANG DIPERBAIKI ---
            # Query ini lebih efisien dan logis:
            # 1. Menggunakan JOIN langsung ke 3 tabel (Order, Order_Item, Products).
            # 2. Menambahkan kondisi WHERE o.status = 'Completed', sehingga hanya produk
            #    dari pesanan yang sudah selesai yang bisa diulas.
            q = """
                SELECT DISTINCT p.product_id, p.name 
                FROM `Order` o
                JOIN Order_Item oi ON o.order_id = oi.order_id
                JOIN Products p ON oi.product_id = p.product_id
                WHERE o.consumer_id = %s AND o.status = 'Completed'
            """
            cursor.execute(q, (consumer_id,))
            purchased = cursor.fetchall()
            
            if not purchased:
                print("Anda belum memiliki produk dari pesanan yang selesai untuk diulas.")
                input("\nTekan Enter...")
                return

            print("Produk yang pernah dibeli (dari pesanan yang 'Completed'):")
            print(tabulate(purchased, headers="keys", tablefmt="grid"))

        product_id = safe_int_input("\nID produk yang akan diulas: ")
        
        # Validasi apakah produk yang diinput ada di daftar yang sah
        if not any(p['product_id'] == product_id for p in purchased):
            raise ValueError("ID Produk tidak valid atau bukan dari pesanan yang telah selesai.")

        rating = safe_int_input("Rating (1-5): ")
        if not 1 <= rating <= 5:
            raise ValueError("Rating harus antara 1 dan 5.")
            
        komentar = input("Komentar: ")

        # Cek apakah pengguna sudah pernah mereview produk ini sebelumnya
        reviews_collection = mongo_db["reviews"]
        existing_review = reviews_collection.find_one({
            "id_produk_sql": product_id,
            "id_konsumen_sql": consumer_id
        })

        if existing_review:
            print("\n[!] Anda sudah pernah memberikan ulasan untuk produk ini.")
        else:
            review_data = {
                "id_produk_sql": product_id, 
                "id_konsumen_sql": consumer_id, 
                "rating": rating, 
                "komentar": komentar, 
                "tanggal": datetime.now()
            }
            reviews_collection.insert_one(review_data)
            print("\n[✓] Ulasan Anda telah disimpan. Terima kasih!")

    except (ValueError, Error) as e:
        print(f"\n[!] Gagal: {e}")
    
    input("\nTekan Enter...")

def show_consumer_menu(conn, mongo_db, user_data):
    consumer_id = user_data['Consumer_ID']
    while True:
        clear_screen()
        print(f"--- Menu Konsumen ({user_data['name']}) ---")
        print("1. Lihat Produk")
        print("2. Tambah ke Keranjang")
        print("3. Lihat Keranjang")          # <-- OPSI BARU
        print("4. Checkout")                # Nomor diubah
        print("5. Riwayat Pembelian")       # Nomor diubah
        print("6. Beri Ulasan")             # Nomor diubah
        print("7. Profil Saya")             # Nomor diubah
        print("8. Logout")                  # Nomor diubah

        choice = input("Pilih: ")
        if choice == '1':
            view_products_consumer(conn, mongo_db)
        elif choice == '2':
            add_to_cart(conn, mongo_db, consumer_id)
        elif choice == '3':
            view_cart(conn, mongo_db, consumer_id) # <-- PANGGIL FUNGSI BARU
        elif choice == '4':
            checkout(conn, mongo_db, consumer_id)
        elif choice == '5':
            view_purchase_history(conn, consumer_id)
        elif choice == '6':
            add_review(conn, mongo_db, consumer_id)
        elif choice == '7':
            manage_profile(conn, consumer_id)
        elif choice == '8':
            break

# ==============================================================================
# FUNGSI UTAMA DAN AUTENTIKASI
# ==============================================================================
def register_user(conn, role_type):
    clear_screen(); table = "Consumer" if role_type == "consumer" else "Worker"
    try:
        print(f"--- PENDAFTARAN {role_type.upper()} BARU ---"); name = input("Nama: "); email = input("Email: "); password = getpass.getpass("Password: "); phone = input("Telepon: ")
        if not all([name, email, password]): print("\n[!] Data Wajib tidak boleh kosong!"); return
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (name, email, password, phone) VALUES (%s, %s, %s, %s)", (name, email, password, phone)); conn.commit()
            print(f"\n[✓] Pendaftaran {role_type.capitalize()} berhasil!")
    except Error as e:
        if e.errno == 1062: print(f"\n[!] Gagal: Email '{email}' sudah terdaftar.")
        else: print(f"\n[!] Error: {e}")
    input("Tekan Enter...")

def login_user(conn, role_type):
    clear_screen()
    # PERBAIKAN: Pastikan id_col sesuai 100% dengan nama kolom di SQL
    id_col = "Consumer_ID" if role_type == "consumer" else "worker_id"
    table = "consumer" if role_type == "consumer" else "worker"
    try:
        print(f"--- LOGIN {role_type.upper()} ---"); email = input("Email: "); password = getpass.getpass("Password: ")
        with conn.cursor(buffered=True, dictionary=True) as cursor:
            # Query f-string aman di sini karena table dan id_col dikontrol internal
            cursor.execute(f"SELECT `{id_col}`, `name` FROM `{table}` WHERE `email` = %s AND `password` = %s", (email, password))
            user = cursor.fetchone()
            if user:
                print(f"Login berhasil! Selamat datang, {user['name']}."); input("Tekan Enter...")
                return user
            else:
                print("Login gagal."); input("\nTekan Enter..."); return None
    except Error as e:
        print(f"Error: {e}"); return None

def main():
    db_conn = get_db_connection(DB_CONFIG); mongo_db = get_mongo_client(MONGO_URI, MONGO_DB_NAME)
    if db_conn is None or mongo_db is None: print("Koneksi database gagal. Aplikasi berhenti."); return
    while True:
        clear_screen(); print("\n===== SELAMAT DATANG DI APLIKASI E-COMMERCE ====="); print("1. Register Konsumen"); print("2. Login Konsumen"); print("3. Register Worker"); print("4. Login Worker"); print("5. Keluar")
        choice = input("Pilih: ")
        if choice == '1': register_user(db_conn, "consumer")
        elif choice == '2':
            user = login_user(db_conn, "consumer");
            if user: show_consumer_menu(db_conn, mongo_db, user)
        elif choice == '3': register_user(db_conn, "worker")
        elif choice == '4':
            user = login_user(db_conn, "worker");
            if user: show_worker_menu(db_conn, mongo_db, user)
        elif choice == '5': break
    if db_conn.is_connected(): db_conn.close()
    print("Terima kasih!")

if __name__ == "__main__":
    main()
