#include <iostream>
#include <stdexcept>
#include <string>
#include <memory>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>

using namespace std;
using sql::Connection;it i
using sql::Driver;
using sql::PreparedStatement;
using sql::ResultSet;

const string DB_HOST = "tcp://127.0.0.1:3306";
const string DB_USER = "root";          // Ubah jika usermu beda
const string DB_PASS = "password";      // Ganti dengan password MySQL kamu
const string DB_NAME = "ecommerce";     // Pastikan database ini sudah ada

void registerUser(Connection* con, const string& role) {
    string name, email, password, phone;

    cout << "=== REGISTER " << role << " ===\n";
    cout << "Nama     : "; getline(cin, name);
    cout << "Email    : "; getline(cin, email);
    cout << "Password : "; getline(cin, password);
    cout << "Phone    : "; getline(cin, phone);

    string table = (role == "consumer") ? "consumer" : "worker";

    unique_ptr<PreparedStatement> stmt(
        con->prepareStatement("INSERT INTO " + table + " (name, email, password, phone) VALUES (?, ?, ?, ?)")
    );
    stmt->setString(1, name);
    stmt->setString(2, email);
    stmt->setString(3, password);
    stmt->setString(4, phone);

    stmt->execute();
    cout << role << " berhasil terdaftar!\n";
}

bool loginUser(Connection* con, const string& role) {
    string email, password;
    cout << "=== LOGIN " << role << " ===\n";
    cout << "Email    : "; getline(cin, email);
    cout << "Password : "; getline(cin, password);

    string table = (role == "consumer") ? "consumer" : "worker";

    unique_ptr<PreparedStatement> stmt(
        con->prepareStatement("SELECT * FROM " + table + " WHERE email = ? AND password = ?")
    );
    stmt->setString(1, email);
    stmt->setString(2, password);

    unique_ptr<ResultSet> res(stmt->executeQuery());
    if (res->next()) {
        cout << "Login berhasil. Selamat datang, " << res->getString("name") << "!\n";
        return true;
    } else {
        cout << "Login gagal. Email atau password salah.\n";
        return false;
    }
}

int main() {
    try {
        Driver* driver = sql::mysql::get_driver_instance();
        unique_ptr<Connection> con(driver->connect(DB_HOST, DB_USER, DB_PASS));
        con->setSchema(DB_NAME);

        while (true) {
            cout << "\n=== MENU UTAMA ===\n";
            cout << "1. Register Consumer\n";
            cout << "2. Register Worker\n";
            cout << "3. Login Consumer\n";
            cout << "4. Login Worker\n";
            cout << "5. Keluar\n";
            cout << "Pilih: ";

            string pilihan;
            getline(cin, pilihan);

            if (pilihan == "1") registerUser(con.get(), "consumer");
            else if (pilihan == "2") registerUser(con.get(), "worker");
            else if (pilihan == "3") loginUser(con.get(), "consumer");
            else if (pilihan == "4") loginUser(con.get(), "worker");
            else if (pilihan == "5") break;
            else cout << "Pilihan tidak valid!\n";
        }

    } catch (sql::SQLException& e) {
        cerr << "SQL Error: " << e.what() << endl;
    } catch (exception& e) {
        cerr << "Error: " << e.what() << endl;
    }

    return 0;
}
