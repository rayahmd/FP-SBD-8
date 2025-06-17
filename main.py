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
        with conn.cursor(dictionary=True) as cursor:
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
        with conn.cursor(dictionary=True) as cursor:
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

def manage_variations(conn, product_id):
    while True:
        clear_screen()
        try:
            with conn.cursor(dictionary=True) as cursor:
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
        with conn.cursor(dictionary=True) as cursor:
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
        with conn.cursor(dictionary=True) as cursor:
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
            with conn.cursor(dictionary=True) as cursor: cursor.execute("SELECT * FROM Promotions"); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
            input("\nTekan Enter...")
        elif choice == '2': add_promotion(conn)
        elif choice == '3': link_promotion_to(conn, "product")
        elif choice == '4': link_promotion_to(conn, "category")
        elif choice == '5': break

def manage_orders(conn):
    while True:
        clear_screen(); print("--- Manajemen Pesanan Masuk ---"); cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            query = "SELECT o.order_id, c.name as customer, o.order_date, o.status, o.grand_total FROM `Order` o JOIN Consumer c ON o.consumer_id = c.Consumer_ID ORDER BY o.order_date DESC LIMIT 15"
            cursor.execute(query); orders = cursor.fetchall()
            if not orders: print("Belum ada pesanan."); break
            print(tabulate(orders, headers="keys", tablefmt="grid")); print("\n1. Update Status Pesanan | 2. Kembali")
            choice = input("Pilih: ")
            if choice == '1':
                order_id = safe_int_input("Masukkan order_id: ")
                if not any(o['order_id'] == order_id for o in orders): print("[!] ID Pesanan tidak valid."); continue
                print("Status Baru: a. Shipped | b. Completed | c. Cancelled")
                status_map = {'a': 'Shipped', 'b': 'Completed', 'c': 'Cancelled'}
                new_status = status_map.get(input("Pilih (a/b/c): ").lower())
                if new_status:
                    cursor.execute("UPDATE `Order` SET status = %s WHERE order_id = %s", (new_status, order_id)); conn.commit()
                    print(f"\n[✓] Status pesanan #{order_id} berhasil diubah.")
                else: print("[!] Pilihan status tidak valid.")
            elif choice == '2': break
        except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
        finally:
            if cursor: cursor.close()
        input("Tekan Enter untuk melanjutkan...")

def manage_reviews(conn, mongo_db):
    clear_screen(); print("--- Manajemen Ulasan Pelanggan ---")
    reviews_collection = mongo_db["reviews"]; all_reviews = list(reviews_collection.find())
    if not all_reviews: print("Belum ada ulasan."); input("\nTekan Enter..."); return
    product_ids = list(set([review.get('id_produk_sql') for review in all_reviews if review.get('id_produk_sql') is not None]))
    products = {}
    if product_ids:
        with conn.cursor(dictionary=True) as cursor:
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
        choice = input("Pilih: "); cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            if choice == '1':
                year_month = input("Masukkan periode (YYYY-MM): ")
                # KOREKSI: Menggunakan %s untuk parameter, bukan format string langsung
                query = "SELECT DATE_FORMAT(order_date, '%Y-%m') as periode, COUNT(order_id) as jumlah_pesanan, SUM(grand_total) as total_pendapatan FROM `Order` WHERE DATE_FORMAT(order_date, '%Y-%m') = %s GROUP BY periode;"
                cursor.execute(query, (year_month,)); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
            elif choice == '2':
                query = "SELECT c.name as nama_pelanggan, COUNT(o.order_id) as jumlah_transaksi, SUM(o.grand_total) as total_belanja FROM `Order` o JOIN Consumer c ON o.consumer_id = c.Consumer_ID GROUP BY o.consumer_id ORDER BY total_belanja DESC LIMIT 10;"
                cursor.execute(query); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
            elif choice == '3':
                query = "SELECT c.name as category, COUNT(oi.order_item_id) as items_sold, SUM(oi.total_price_for_item) as category_revenue FROM Order_Item oi JOIN Products p ON oi.product_id = p.product_id JOIN Categories c ON p.category_id = c.category_id GROUP BY c.category_id ORDER BY category_revenue DESC;"
                cursor.execute(query); print(tabulate(cursor.fetchall(), headers="keys", tablefmt="grid"))
            elif choice == '4': break
        except (ValueError, Error) as e: print(f"\n[!] Error: {e}")
        finally:
            if cursor: cursor.close()
        input("\nTekan Enter...")

def show_worker_menu(conn, mongo_db, user_data):
    while True:
        clear_screen(); print(f"--- Menu Worker ({user_data['name']}) ---"); print("1. Manajemen Kategori"); print("2. Manajemen Produk"); print("3. Manajemen Promosi"); print("4. Manajemen Pesanan"); print("5. Manajemen Ulasan"); print("6. Lihat Laporan"); print("7. Logout")
        choice = input("Pilih: ")
        if choice == '1': manage_categories(conn)
        elif choice == '2': manage_products(conn, user_data['worker_id'])
        elif choice == '3': manage_promotions(conn)
        elif choice == '4': manage_orders(conn)
        elif choice == '5': manage_reviews(conn, mongo_db)
        elif choice == '6': view_reports(conn)
        elif choice == '7': break

# ==============================================================================
# FITUR-FITUR CONSUMER
# ==============================================================================

def manage_addresses(conn, consumer_id):
    while True:
        clear_screen(); print("--- Kelola Alamat Pengiriman ---")
        try:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute("SELECT Shipping_Address_ID, recipient_name, address_line1, city, is_default_shipping FROM Shipping_Address WHERE consumer_id = %s", (consumer_id,))
                addresses = cursor.fetchall(); print(tabulate(addresses, headers="keys", tablefmt="grid") if addresses else "Anda belum memiliki alamat.")
            print("\n1. Tambah Alamat Baru | 2. Atur Alamat Default | 3. Hapus Alamat | 4. Kembali")
            choice = input("Pilih: ")
            if choice == '1':
                r = input("Nama Penerima: "); p = input("No. Telepon: "); a1 = input("Alamat: "); c = input("Kota: "); pv = input("Provinsi: "); pc = input("Kode Pos: ")
                with conn.cursor() as cursor:
                    q = "INSERT INTO Shipping_Address (consumer_id, recipient_name, phone_number, address_line1, city, province, postal_code, country) VALUES (%s,%s,%s,%s,%s,%s,%s, 'Indonesia')"
                    cursor.execute(q, (consumer_id, r, p, a1, c, pv, pc)); conn.commit(); print("\n[✓] Alamat baru ditambahkan.")
            elif choice == '2':
                addr_id = safe_int_input("Masukkan ID alamat untuk dijadikan default: ")
                with conn.cursor() as cursor:
                    cursor.execute("UPDATE Shipping_Address SET is_default_shipping = 0 WHERE consumer_id = %s", (consumer_id,))
                    cursor.execute("UPDATE Shipping_Address SET is_default_shipping = 1 WHERE Shipping_Address_ID = %s AND consumer_id = %s", (addr_id, consumer_id))
                    cursor.execute("UPDATE Consumer SET shipping_address_id = %s WHERE Consumer_ID = %s", (addr_id, consumer_id)); conn.commit()
                    print("\n[✓] Alamat default berhasil diubah.")
            elif choice == '3':
                addr_id = safe_int_input("Masukkan ID alamat yang akan dihapus: ")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM Shipping_Address WHERE Shipping_Address_ID = %s AND consumer_id = %s", (addr_id, consumer_id)); conn.commit()
                    if cursor.rowcount > 0: print("\n[✓] Alamat berhasil dihapus.")
                    else: print("\n[!] Alamat tidak ditemukan.")
            elif choice == '4': break
        except (ValueError, Error) as e: print(f"\n[!] Terjadi kesalahan: {e}")
        input("Tekan Enter...")

def manage_profile(conn, consumer_id):
    while True:
        clear_screen(); print("--- Manajemen Profil Saya ---"); print("1. Kelola Alamat Pengiriman"); print("2. Kelola Metode Pembayaran (TBA)"); print("3. Kembali")
        choice = input("Pilih: ")
        if choice == '1': manage_addresses(conn, consumer_id)
        elif choice == '3': break

def view_products_consumer(conn, mongo_db):
    clear_screen(); print("--- Daftar Produk ---")
    with conn.cursor(dictionary=True) as cursor:
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
    carts_collection = mongo_db["carts"]
    try:
        product_id = safe_int_input("Masukkan ID produk: "); variation_id = safe_int_input("Masukkan ID variasi: "); quantity = safe_int_input("Jumlah: ")
        with conn.cursor(dictionary=True) as cursor:
            q = "SELECT p.name as product_name, v.variation_name, v.price_override, p.base_price, v.stock_quantity FROM Product_Variations v JOIN Products p ON v.product_id = p.product_id WHERE v.variation_id = %s"
            cursor.execute(q, (variation_id,)); data = cursor.fetchone()
            if not data: raise ValueError("Variasi produk tidak ditemukan.")
            if quantity > data['stock_quantity']: raise ValueError(f"Stok tidak mencukupi. Sisa stok: {data['stock_quantity']}")
            price = float(data['price_override'] if data['price_override'] else data['base_price'])
        cart_item = {"product_id": product_id, "variation_id": variation_id, "product_name": data['product_name'], "variation_name": data['variation_name'], "quantity": quantity, "price_at_cart": price}
        carts_collection.update_one({"consumer_id": consumer_id}, {"$push": {"items": cart_item}, "$set": {"last_updated": datetime.now()}}, upsert=True)
        print("\n[✓] Produk berhasil ditambahkan ke keranjang!")
    except (ValueError, Error) as e: print(f"\n[!] Gagal: {e}")
    input("Tekan Enter...")

def checkout(conn, mongo_db, consumer_id):
    carts_collection = mongo_db["carts"]; cart = carts_collection.find_one({"consumer_id": consumer_id})
    if not cart or not cart.get("items"): print("Keranjang kosong."); input("\nTekan Enter..."); return
    clear_screen(); print("--- Checkout ---"); print(tabulate(cart['items'], headers="keys", tablefmt="grid"))
    subtotal = sum(item['price_at_cart'] * item['quantity'] for item in cart['items']); print(f"\nSubtotal: Rp {subtotal:,.2f}"); cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        discount_amount = 0; promotion_id = None;
        promo_code = input("Masukkan kode promosi (opsional): ").upper()
        if promo_code:
            cursor.execute("SELECT * FROM Promotions WHERE code = %s AND is_active = 1 AND NOW() BETWEEN start_date AND end_date", (promo_code,))
            promo = cursor.fetchone()
            if promo and subtotal >= float(promo['min_purchase_amount']):
                promotion_id = promo['promotion_id']
                if promo['discount_type'] == 'PERCENTAGE': discount_amount = (subtotal * float(promo['discount_value']) / 100)
                else: discount_amount = float(promo['discount_value'])
                print(f"[✓] Promosi '{promo_code}' diterapkan! Diskon: - Rp {discount_amount:,.2f}")
        # (Logika promosi otomatis bisa ditambahkan di sini jika promo manual tidak ada)
        print("\n--- Pilih Metode Pengiriman ---")
        cursor.execute("SELECT shipping_method_id, shipping_name, shipping_cost FROM Shipping_Methods WHERE is_active = 1"); shipping_methods = cursor.fetchall()
        if not shipping_methods: print("[!] Tidak ada metode pengiriman."); return
        print(tabulate(shipping_methods, headers="keys", tablefmt="grid")); chosen_shipping_id = safe_int_input("Masukkan ID pengiriman: ")
        chosen_method = next((m for m in shipping_methods if m['shipping_method_id'] == chosen_shipping_id), None)
        if not chosen_method: print("[!] ID tidak valid."); return
        shipping_cost = float(chosen_method['shipping_cost']); grand_total = (subtotal - discount_amount) + shipping_cost
        print(f"\nBiaya Pengiriman: Rp {shipping_cost:,.2f}"); print(f"Grand Total: Rp {grand_total:,.2f}")
        if input("Lanjutkan pembayaran (y/n)? ").lower() != 'y': return
        conn.start_transaction()
        # Mengambil snapshot alamat dan pembayaran default
        cursor.execute("SELECT s.address_line1, s.city, s.province, p.method_type FROM Consumer c LEFT JOIN Shipping_Address s ON c.shipping_address_id = s.Shipping_Address_ID LEFT JOIN Payment_Methods p ON c.payment_method_id = p.Payment_method_id WHERE c.Consumer_ID = %s", (consumer_id,))
        snapshot_data = cursor.fetchone(); shipping_snapshot = f"{snapshot_data['address_line1']}, {snapshot_data['city']}" if snapshot_data else "Alamat tidak diatur"; payment_snapshot = snapshot_data['method_type'] if snapshot_data else "N/A"
        order_query = "INSERT INTO `Order` (consumer_id, shipping_method_id, promotion_id, order_date, status, grand_total, subtotal_amount, discount_amount, shipping_costs_charged, shipping_address_snapshot, payment_method_snapshot) VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(order_query, (consumer_id, chosen_shipping_id, promotion_id, 'Processing', grand_total, subtotal, discount_amount, shipping_cost, shipping_snapshot, payment_snapshot))
        order_id = cursor.lastrowid
        for item in cart['items']:
            stock_query = "UPDATE Product_Variations SET stock_quantity = stock_quantity - %s WHERE variation_id = %s AND stock_quantity >= %s"
            cursor.execute(stock_query, (item['quantity'], item['variation_id'], item['quantity']))
            if cursor.rowcount == 0: raise Exception(f"Stok untuk '{item['product_name']}' habis!")
            item_query = "INSERT INTO Order_Item (order_id, product_id, variation_id, quantity, price_per_unit_snapshot, product_name_snapshot, variation_name_snapshot, total_price_for_item) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(item_query, (order_id, item['product_id'], item['variation_id'], item['quantity'], item['price_at_cart'], item['product_name'], item['variation_name'], item['price_at_cart'] * item['quantity']))
        conn.commit(); carts_collection.delete_one({"consumer_id": consumer_id}); print("\n[✓] Checkout berhasil!")
    except (ValueError, Error) as e: print(f"\n[!] Checkout GAGAL: {e}"); conn.rollback()
    finally:
        if cursor: cursor.close()
    input("\nTekan Enter...")

def view_purchase_history(conn, consumer_id):
    clear_screen(); print("--- Riwayat Pembelian Anda ---"); cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
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
    clear_screen(); print("--- Beri Ulasan Produk ---")
    try:
        with conn.cursor(dictionary=True) as cursor:
            q = "SELECT DISTINCT p.product_id, p.name FROM Order_Item oi JOIN Products p ON oi.product_id = p.product_id WHERE oi.order_id IN (SELECT order_id FROM `Order` WHERE consumer_id = %s)"
            cursor.execute(q, (consumer_id,)); purchased = cursor.fetchall()
            if not purchased: print("Anda harus membeli produk dulu."); return
            print("Produk yang pernah dibeli:"); print(tabulate(purchased, headers="keys", tablefmt="grid"))
        product_id = safe_int_input("\nID produk yang akan diulas: ")
        if not any(p['product_id'] == product_id for p in purchased): raise ValueError("Anda tidak pernah membeli produk ini.")
        rating = safe_int_input("Rating (1-5): ");
        if not 1 <= rating <= 5: raise ValueError("Rating harus antara 1 dan 5.")
        komentar = input("Komentar: ")
        reviews_collection = mongo_db["reviews"]; review_data = {"id_produk_sql": product_id, "id_konsumen_sql": consumer_id, "rating": rating, "komentar": komentar, "tanggal": datetime.now()}
        reviews_collection.insert_one(review_data); print("\n[✓] Ulasan Anda telah disimpan.")
    except (ValueError, Error) as e: print(f"\n[!] Gagal: {e}")
    input("\nTekan Enter...")

def show_consumer_menu(conn, mongo_db, user_data):
    consumer_id = user_data['Consumer_ID']
    while True:
        clear_screen(); print(f"--- Menu Konsumen ({user_data['name']}) ---"); print("1. Lihat Produk"); print("2. Tambah ke Keranjang"); print("3. Checkout"); print("4. Riwayat Pembelian"); print("5. Beri Ulasan"); print("6. Profil Saya"); print("7. Logout")
        choice = input("Pilih: ")
        if choice == '1': view_products_consumer(conn, mongo_db)
        elif choice == '2': add_to_cart(conn, mongo_db, consumer_id)
        elif choice == '3': checkout(conn, mongo_db, consumer_id)
        elif choice == '4': view_purchase_history(conn, consumer_id)
        elif choice == '5': add_review(conn, mongo_db, consumer_id)
        elif choice == '6': manage_profile(conn, consumer_id)
        elif choice == '7': break

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
    clear_screen(); id_col = "Consumer_ID" if role_type == "consumer" else "worker_id"; table = "Consumer" if role_type == "consumer" else "Worker"
    try:
        print(f"--- LOGIN {role_type.upper()} ---"); email = input("Email: "); password = getpass.getpass("Password: ")
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute(f"SELECT {id_col}, name FROM {table} WHERE email = %s AND password = %s", (email, password)); user = cursor.fetchone()
            if user: print(f"Login berhasil! Selamat datang, {user['name']}."); input("Tekan Enter..."); return user
            else: print("Login gagal."); input("\nTekan Enter..."); return None
    except Error as e: print(f"Error: {e}"); return None

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
