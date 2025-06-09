import mysql.connector
from mysql.connector import Error
import getpass # Untuk menyembunyikan input password

# --- Konfigurasi Database ---
# (Pastikan database 'ecommerce' dan tabelnya sudah dibuat dari langkah C++ sebelumnya)
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '', # Ganti dengan password MySQL Anda
    'database': 'ecommerce'
}

def connect_to_db():
    """Mencoba terhubung ke database dan mengembalikan object koneksi."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("Koneksi ke database berhasil.")
            return conn
    except Error as e:
        print(f"Error saat menghubungkan ke MySQL: {e}")
        return None

def register_user(conn, role):
    """Mendaftarkan user baru (consumer atau worker)."""
    cursor = conn.cursor()
    try:
        print(f"\n=== REGISTER {role.upper()} ===")
        name = input("Nama     : ")
        email = input("Email    : ")
        # Menggunakan getpass agar password tidak terlihat saat diketik
        password = getpass.getpass("Password : ")
        phone = input("Phone    : ")

        if not all([name, email, password]):
            print("Nama, email, dan password tidak boleh kosong!")
            return

        table = "consumer" if role == "consumer" else "worker"
        
        # PENTING: Menggunakan parameterized query untuk keamanan (mencegah SQL Injection)
        query = f"INSERT INTO {table} (name, email, password, phone) VALUES (%s, %s, %s, %s)"
        params = (name, email, password, phone)
        
        cursor.execute(query, params)
        conn.commit() # Simpan perubahan ke database
        
        print(f"{role.capitalize()} berhasil terdaftar!")

    except Error as e:
        if e.errno == 1062: # Error code untuk duplicate entry
            print(f"Error: Email '{email}' sudah terdaftar.")
        else:
            print(f"Terjadi error pada database: {e}")
    finally:
        cursor.close()


def login_user(conn, role):
    """Login user dan mengembalikan ID jika berhasil, None jika gagal."""
    cursor = conn.cursor(dictionary=True) # dictionary=True agar hasil query seperti objek
    try:
        print(f"\n=== LOGIN {role.upper()} ===")
        email = input("Email    : ")
        password = getpass.getpass("Password : ")
        
        table = "consumer" if role == "consumer" else "worker"
        id_column = "consumer_id" if role == "consumer" else "worker_id"

        query = f"SELECT {id_column}, name FROM {table} WHERE email = %s AND password = %s"
        params = (email, password)
        
        cursor.execute(query, params)
        user = cursor.fetchone() # Ambil satu baris hasil
        
        if user:
            print(f"Login berhasil. Selamat datang, {user['name']}!")
            return user # Mengembalikan data user (dict)
        else:
            print("Login gagal. Email atau password salah.")
            return None
            
    except Error as e:
        print(f"Terjadi error pada database: {e}")
        return None
    finally:
        cursor.close()

def show_consumer_menu(conn, user_data):
    """Placeholder untuk menu consumer setelah login."""
    print(f"\n--- Menu Konsumen (Login sebagai: {user_data['name']}) ---")
    print("1. Lihat Produk")
    print("2. Keranjang Saya")
    print("3. Riwayat Pembelian")
    print("4. Logout")
    # Logika menu consumer akan ada di sini
    print("Fitur belum diimplementasikan.")


def show_worker_menu(conn, user_data):
    """Placeholder untuk menu worker setelah login."""
    print(f"\n--- Menu Worker (Login sebagai: {user_data['name']}) ---")
    print("1. Atur Produk")
    print("2. Lihat Laporan Penjualan")
    print("3. Manajemen Member")
    print("4. Logout")
    # Logika menu worker akan ada di sini
    print("Fitur belum diimplementasikan.")

# --- Fungsi Utama Program ---
def main():
    db_connection = connect_to_db()
    if not db_connection:
        return # Keluar jika koneksi database gagal

    while True:
        print("\n===== MENU UTAMA E-COMMERCE (PYTHON) =====")
        print("1. Register Consumer")
        print("2. Login Consumer")
        print("3. Register Worker")
        print("4. Login Worker")
        print("5. Keluar")
        
        pilihan = input("Pilih: ")

        if pilihan == '1':
            register_user(db_connection, "consumer")
        elif pilihan == '2':
            user = login_user(db_connection, "consumer")
            if user:
                show_consumer_menu(db_connection, user)
        elif pilihan == '3':
            register_user(db_connection, "worker")
        elif pilihan == '4':
            user = login_user(db_connection, "worker")
            if user:
                show_worker_menu(db_connection, user)
        elif pilihan == '5':
            print("Terima kasih telah menggunakan aplikasi.")
            break
        else:
            print("Pilihan tidak valid!")

    # Tutup koneksi saat program selesai
    if db_connection and db_connection.is_connected():
        db_connection.close()
        print("Koneksi ke database ditutup.")

# Menjalankan fungsi utama
if __name__ == "__main__":
    main()
