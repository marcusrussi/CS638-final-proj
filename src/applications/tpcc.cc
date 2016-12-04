
#include "applications/tpcc.h"

#include <iostream>
#include "backend/storage.h"
#include "common/utils.h"

//----------------------------------------------------
void TPCC::NewTxn(Txn* txn, uint64_t txn_id) const {
  int percent = rand() % 100;
  int first_lm_id = rand() % lm_count_;

  if (percent < 50) {
    NewOrderTxn(txn, txn_id, first_lm_id);
  } else {
    PaymentTxn(txn, txn_id, first_lm_id);
  }
}

//--------------------------------------------------------

void TPCC::NewOrderTxn(Txn* txn, uint64_t txn_id, int part1) const {
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(NEW_ORDER);
  txn->SetTxnStatus(ACTIVE);

  uint64_t w_id;
  do {
    w_id = (uint64_t)random.gen_rand_range(0, s_num_warehouses-1);
  } while (w_id % lm_count_ != (uint64_t)part1);

  uint64_t d_id = (uint64_t)random.gen_rand_range(0, s_districts_per_wh-1);
  uint64_t c_id = (uint64_t)random.gen_rand_range(0, s_customers_per_dist-1);

  txn->AddReadSet(0, w_id);

  uint32_t d_keys[2];
  d_keys[0] = w_id;
  d_keys[1] = d_id;
  txn->AddReadWriteSet(1, TPCCKeyGen::create_district_key(d_keys));

  uint32_t c_keys[3];
  c_keys[0] = w_id;
  c_keys[1] = d_id;
  c_keys[2] = c_id;
  txn->AddReadSet(2, TPCCKeyGen::create_customer_key(c_keys));

  //uint32_t num_items = random.gen_rand_range(5, 15);
  //txn->tpcc_args.num_items = num_items;
  uint32_t num_items = 10;

  /**for (uint32_t i = 0; i < num_items; ++i) {
    //quantities[i] = random.gen_rand_range(1, 10);
    txn->tpcc_args.neworder_quantities[i] = random.gen_rand_range(1, 10);
  }**/

  uint32_t supplier_wh_id;

  set<uint64_t> seen_items;
  for (uint32_t i = 0; i < num_items; i++) {
    uint64_t cur_item;
    do {
      cur_item = (uint64_t)random.gen_rand_range(0, s_num_items-1);
    } while (seen_items.find(cur_item) != seen_items.end());
    seen_items.insert(cur_item);

    int pct = random.gen_rand_range(1, 100);
    //if (pct > -1) {
    if (pct > 1) {
      supplier_wh_id = w_id;
    } else {
      do {
        supplier_wh_id = random.gen_rand_range(0, s_num_warehouses-1);
      } while (supplier_wh_id == w_id && s_num_warehouses > 1);
    }

    uint32_t stock_keys[2];
    stock_keys[0] = supplier_wh_id;
    stock_keys[1] = cur_item;
    txn->AddReadWriteSet(8, TPCCKeyGen::create_stock_key(stock_keys));
  }

/**std::cout<<"------------ before------------\n"<<std::flush;
  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    std::cout<<i<<": "<<txn->GetReadWriteSet(i).table_id<<":  "<<txn->GetReadWriteSet(i).key<<".  "<<LookupPartition(txn->GetReadWriteSet(i).key)<<".\n"<<std::flush;
  }**/
}

//-----------------------------------------------------------------
void TPCC::PaymentTxn(Txn* txn, uint64_t txn_id, int part1) const {
  // Set the transaction's standard attributes
  txn->SetTxnId(txn_id);
  txn->SetTxnType(PAYMENT);
  txn->SetTxnStatus(ACTIVE);

  uint64_t w_id;
  do {
    w_id = (uint64_t)random.gen_rand_range(0, s_num_warehouses-1);
  } while (w_id % lm_count_ != (uint64_t)part1);

  uint64_t d_id = (uint64_t)random.gen_rand_range(0, s_districts_per_wh-1);

  txn->AddReadWriteSet(0, w_id);
//txn->AddReadSet(0, w_id);

  uint32_t d_keys[2];
  d_keys[0] = w_id;
  d_keys[1] = d_id;
  txn->AddReadWriteSet(1, TPCCKeyGen::create_district_key(d_keys));


  uint64_t c_id = (uint64_t)random.gen_rand_range(0, s_customers_per_dist-1);
  uint32_t c_d_id;
  uint32_t c_w_id;
  int x = random.gen_rand_range(1, 100);

  //if (x <= 101 || (s_num_warehouses == 1)) {
  if (x <= 85 || (s_num_warehouses == 1)) {
    c_d_id = d_id;
    c_w_id = w_id;
  } else {
    c_d_id = (uint32_t)random.gen_rand_range(0, s_districts_per_wh-1);
    do {
      c_w_id = (uint32_t)random.gen_rand_range(0, s_num_warehouses-1);
    } while (c_w_id == w_id);
  }

  uint32_t c_keys[3];
  c_keys[0] = c_w_id;
  c_keys[1] = c_d_id;
  c_keys[2] = c_id;
  uint64_t customer_key = TPCCKeyGen::create_customer_key(c_keys);
  txn->AddReadWriteSet(2, customer_key);

  //float payment_amt = (float)random.gen_rand_range(100, 500000)/100.0;
  //txn->tpcc_args.payment_amount = payment_amt;
}

//------------------------------------------------------------------
int TPCC::Execute(Txn* txn) const {
  if (txn->GetTxnType() == NEW_ORDER) {
    ExecuteNewOrderTxn(txn);
  } else {
    ExecutePaymentTxn(txn);
  }
  return 0;
}

//------------------------------------------------------------------

int TPCC::ExecuteNewOrderTxn(Txn* txn) const {

  TableKey table_key = txn->GetReadSet(0);
  assert(table_key.table_id == 0);
  Warehouse* warehouse = (Warehouse*)storage_->ReadRecord(table_key.table_id, table_key.key);
  float w_tax = warehouse->w_tax;
  uint32_t w_id = TPCCKeyGen::get_warehouse_key(table_key.key);

  TableKey district_table_key(1,0);
  TableKey stock_table_key[10];
  int j = 0;
  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    if (txn->GetReadWriteSet(i).table_id == 1) {
      district_table_key = txn->GetReadWriteSet(i);
    } else {
      stock_table_key[j++] = txn->GetReadWriteSet(i);
    }
  }

  District* district = (District*)storage_->ReadRecord(district_table_key.table_id, district_table_key.key);
  float d_tax = district->d_tax;
  uint32_t order_id = district->d_next_o_id;
  district->d_next_o_id += 1;
  uint32_t d_id = TPCCKeyGen::get_district_key(district_table_key.key);

  table_key = txn->GetReadSet(1);
  assert(table_key.table_id == 2);
//int x_w = TPCCKeyGen::get_warehouse_key(table_key.key);
//int x_d = TPCCKeyGen::get_district_key(table_key.key);
//int x_c = TPCCKeyGen::get_customer_key(table_key.key);
//std::cout<<"customer, w:"<<x_w<<". d:"<<x_d<<". x_c:"<<x_c<<".\n"<<std::flush;
  Customer* customer = (Customer*)storage_->ReadRecord(table_key.table_id, table_key.key);
  float c_discount = customer->c_discount;
  //char* c_lastname = customer->c_last;
  //char* c_credit = customer->c_credit;
  uint32_t c_id = TPCCKeyGen::get_customer_key(table_key.key);

  // Insert into NewOrder table
  NewOrder new_order;
  new_order.no_o_id = order_id;
  new_order.no_d_id = d_id;
  new_order.no_w_id = w_id;
  uint32_t keys[4];
  keys[0] = w_id;
  keys[1] = d_id;
  keys[2] = order_id;
  uint64_t new_order_key = TPCCKeyGen::create_new_order_key(keys);
  storage_->InsertRecord(txn->GetWorkerId(), 4, new_order_key, (void*)&new_order);

  //uint32_t num_items = txn->tpcc_args.num_items;
  uint32_t num_items = 10;
  float total_amount = 0;
  bool is_local = true;

  for (uint32_t i = 0; i < num_items; i++) {
    assert(stock_table_key[i].table_id == 8);

    uint32_t item_id = TPCCKeyGen::get_stock_key(stock_table_key[i].key);
    Item* item = (Item*)storage_->ReadRecord(7, item_id);
    Stock* stock = (Stock*)storage_->ReadRecord(stock_table_key[i].table_id, stock_table_key[i].key);

    float i_price = item->i_price;
    //char* i_name = item->i_name;
    //char* i_data = item->i_data;

    uint32_t s_quantity = stock->s_quantity;
    uint32_t s_w_id = TPCCKeyGen::get_warehouse_key(stock_table_key[i].key);
    if (s_w_id != w_id) {
      is_local = false;
      stock->s_remote_cnt += 1;
    }
    stock->s_order_cnt += 1;

    /**if (s_quantity - txn->tpcc_args.neworder_quantities[i] >= 10) {
      stock->s_quantity = s_quantity - txn->tpcc_args.neworder_quantities[i];
    } else {
      stock->s_quantity = s_quantity - txn->tpcc_args.neworder_quantities[i] + 91;
    }**/

    if (s_quantity - i >= 10) {
      stock->s_quantity = s_quantity - i;
    } else {
      stock->s_quantity = s_quantity - i + 91;
    }

    char *ol_dist_info = NULL;
    switch (d_id) {
      case 0:
        ol_dist_info = stock->s_dist_01;
        break;
      case 1:
        ol_dist_info = stock->s_dist_02;
        break;
      case 2:
        ol_dist_info = stock->s_dist_03;
        break;
      case 3:
        ol_dist_info = stock->s_dist_04;
        break;
      case 4:
        ol_dist_info = stock->s_dist_05;
        break;
      case 5:
        ol_dist_info = stock->s_dist_06;
        break;
      case 6:
        ol_dist_info = stock->s_dist_07;
        break;
      case 7:
        ol_dist_info = stock->s_dist_08;
        break;
      case 8:
        ol_dist_info = stock->s_dist_09;
        break;
      case 9:
        ol_dist_info = stock->s_dist_10;
        break;
      default:
        std::cout << "Got unexpected district!!! Aborting...\n";
        std::cout << d_id << "\n";
        exit(0);
      }

      //stock->s_ytd += txn->tpcc_args.neworder_quantities[i];
      //total_amount += txn->tpcc_args.neworder_quantities[i] * i_price;

      stock->s_ytd += i;
      total_amount += i * i_price;

      OrderLine new_order_line;
      new_order_line.ol_o_id = order_id;
      new_order_line.ol_d_id = d_id;
      new_order_line.ol_w_id = w_id;
      new_order_line.ol_number = i;
      new_order_line.ol_i_id = item_id;
      new_order_line.ol_supply_w_id = s_w_id;
      //new_order_line.ol_quantity = txn->tpcc_args.neworder_quantities[i];
      //new_order_line.ol_amount = txn->tpcc_args.neworder_quantities[i] * i_price;

      new_order_line.ol_quantity = i;
      new_order_line.ol_amount = i * i_price;

      new_order_line.ol_delivery_d = 0;
      strcpy(new_order_line.ol_dist_info, ol_dist_info);

      keys[3] = i;
      uint64_t order_line_key = TPCCKeyGen::create_order_line_key(keys);
      storage_->InsertRecord(txn->GetWorkerId(), 6, order_line_key, (void*)&new_order_line);
  }

  total_amount = total_amount * (1 - c_discount) * (1 + w_tax + d_tax);

  // Insert into Order table
  Order order;
  order.o_id = order_id;
  order.o_w_id = w_id;
  order.o_d_id = d_id;
  order.o_c_id = c_id;
  order.o_carrier_id = 0;
  order.o_ol_cnt = num_items;
  if (is_local == true) {
    order.o_all_local = 1;
  } else {
    order.o_all_local = 0;
  }
  order.o_entry_d = 0;
  storage_->InsertRecord(txn->GetWorkerId(), 5, new_order_key, (void*)&order);

  return 0;
}

//------------------------------------------------------------------
int TPCC::ExecutePaymentTxn(Txn* txn) const {

  //float h_amount = txn->tpcc_args.payment_amount;
  float h_amount = 100;

  TableKey warehouse_table_key(0, 0);
  TableKey district_table_key(1, 0);
  TableKey customer_table_key(2, 0);

  for (uint32_t i = 0; i < txn->ReadWriteSetSize(); i++) {
    if (txn->GetReadWriteSet(i).table_id == 0) {
      warehouse_table_key = txn->GetReadWriteSet(i);
    } else if (txn->GetReadWriteSet(i).table_id == 1) {
      district_table_key = txn->GetReadWriteSet(i);
    } else {
      customer_table_key = txn->GetReadWriteSet(i);
    }
  }

  Warehouse* warehouse = (Warehouse*)storage_->ReadRecord(warehouse_table_key.table_id, warehouse_table_key.key);
  uint32_t w_id = TPCCKeyGen::get_warehouse_key(warehouse_table_key.key);
  warehouse->w_ytd += h_amount;

  char* w_name = warehouse->w_name;
  //char* w_street_1 = warehouse->w_street_1;
  //char* w_street_2 = warehouse->w_street_2;
  //char* w_city = warehouse->w_city;
  //char* w_state = warehouse->w_state;
  //char* w_zip = warehouse->w_zip;

  District* district = (District*)storage_->ReadRecord(district_table_key.table_id, district_table_key.key);
  uint32_t d_id = TPCCKeyGen::get_district_key(district_table_key.key);
  district->d_ytd += h_amount;

  char* d_name = district->d_name;
  //char* d_street_1 = district->d_street_1;
  //char* d_street_2 = district->d_street_2;
  //char* d_city = district->d_city;
  //char* d_state = district->d_state;
  //char* d_zip = district->d_zip;

  Customer* customer = (Customer*)storage_->ReadRecord(customer_table_key.table_id, customer_table_key.key);
  uint32_t c_id = TPCCKeyGen::get_customer_key(customer_table_key.key);
  uint32_t c_w_id = TPCCKeyGen::get_warehouse_key(customer_table_key.key);
  uint32_t c_d_id = TPCCKeyGen::get_district_key(customer_table_key.key);

  customer->c_balance -= h_amount;
  customer->c_ytd_payment += h_amount;
  customer->c_payment_cnt += 1;

  //char* c_first = customer->c_first;
  //char* c_middle = customer->c_middle;
  //char* c_last = customer->c_last;
  //char* c_street_1 = customer->c_street_1;
  //char* c_street_2 = customer->c_street_2;
  //char* c_city = customer->c_city;
  //char* c_state = customer->c_state;
  //char* c_zip = customer->c_zip;
  //char* c_phone = customer->c_phone;
  //uint64_t c_since = customer->c_since;
  //float c_credit_lim = customer->c_credit_lim;
  //float c_discount = customer->c_discount;


  static const char *credit = "BC";
  if (strcmp(credit, customer->c_credit) == 0) {	// Bad credit
    static const char *space = " ";
    char c_id_str[17];
    sprintf(c_id_str, "%x", c_id);
    char c_d_id_str[17];
    sprintf(c_d_id_str, "%x", c_d_id);
    char c_w_id_str[17];
    sprintf(c_w_id_str, "%x", c_w_id);
    char d_id_str[17];
    sprintf(d_id_str, "%x", w_id);
    char w_id_str[17];
    sprintf(w_id_str, "%x", d_id);
    char h_amount_str[17];
    sprintf(h_amount_str, "%lx", (uint64_t)h_amount);

    static const char *holder[11] = {c_id_str, space, c_d_id_str, space,
                                         c_w_id_str, space, d_id_str, space,
                                         w_id_str, space, h_amount_str};
    random.append_strings(customer->c_data, holder, 501, 11);
  }

  // Insert an item into the History table
  History hist;
  hist.h_c_id = c_id;
  hist.h_c_d_id = c_d_id;
  hist.h_c_w_id = c_w_id;
  hist.h_d_id = d_id;
  hist.h_w_id = w_id;
  hist.h_date = 0;
  hist.h_amount = h_amount;

  static const char *empty = "    ";
  const char *holder[3] = {w_name, empty, d_name};
  random.append_strings(hist.h_data, holder, 26, 3);

  storage_->InsertRecord(txn->GetWorkerId(), 3, customer_table_key.key, (void*)&hist);

  return 0;
}


//------------------------------------------------------------------

uint32_t TPCC::LookupPartition(const uint64_t& key) const {
  return TPCCKeyGen::get_warehouse_key(key) % lm_count_;
}

uint32_t TPCC::GetTableNum() const {
  return 9;
}

//--------------------------------------------------------------------
void TPCC::InitializeStorage() const {

  // 0. First create all tables
  storage_->NewTable(0, s_num_warehouses, s_num_warehouses + 1, sizeof(Warehouse));
  storage_->NewTable(1, s_num_warehouses*s_districts_per_wh, s_num_warehouses*s_districts_per_wh + 1, sizeof(District));
  storage_->NewTable(2, s_num_warehouses*s_districts_per_wh*s_customers_per_dist/1, s_num_warehouses*s_districts_per_wh*s_customers_per_dist + 1, sizeof(Customer));
  storage_->NewTable(3, 1000, 2*TRANSACTIONS_GENERATED/WORKER_THREADS + 1, sizeof(History));
  storage_->NewTable(4, 1000, 2*TRANSACTIONS_GENERATED/WORKER_THREADS + 1, sizeof(NewOrder));
  storage_->NewTable(5, 1000, 2*TRANSACTIONS_GENERATED/WORKER_THREADS + 1, sizeof(Order));
  storage_->NewTable(6, 1000, 2*TRANSACTIONS_GENERATED/1000000*10/WORKER_THREADS + 1, sizeof(OrderLine));
  storage_->NewTable(7, s_num_items, s_num_items + 1, sizeof(Item));
  storage_->NewTable(8, s_num_warehouses*s_districts_per_wh*s_num_items/1, s_num_warehouses*s_districts_per_wh*s_num_items + 1, sizeof(Stock));

  // 1. Generate records for warehouse table
  Warehouse warehouse;
  warehouse.w_id = -1;
  warehouse.w_ytd = 30000.0;
  warehouse.w_tax = (rand() % 2001) / 1000.0;

  random.gen_rand_string(6, 10, warehouse.w_name);
  random.gen_rand_string(10, 20, warehouse.w_street_1);
  random.gen_rand_string(10, 20, warehouse.w_street_2);
  random.gen_rand_string(10, 20, warehouse.w_city);
  random.gen_rand_string(2, 2, warehouse.w_state);
  char stupid_zip[] = "123456789";
  strcpy(warehouse.w_zip, stupid_zip);

  for (uint32_t i = 0; i < s_num_warehouses; ++i) {
    warehouse.w_id = i;
    storage_->PutRecord(0, (uint64_t)i, (void*)&warehouse);
  }

  // 2. Generate records for districts table
  uint32_t d_keys[2];
  District district;

  district.d_id = -1;
  district.d_w_id = -1;
  district.d_ytd = 3000;
  district.d_tax = (rand() % 2001) / 1000.0;
  district.d_next_o_id = 0;

  random.gen_rand_string(6, 10, district.d_name);
  random.gen_rand_string(10, 20, district.d_street_1);
  random.gen_rand_string(10, 20, district.d_street_2);
  random.gen_rand_string(10, 20, district.d_city);
  random.gen_rand_string(2, 2, district.d_state);
  char contiguous_zip[] = "123456789";
  strcpy(district.d_zip, contiguous_zip);

  for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
    d_keys[0] = w_id;
    for (uint32_t i = 0; i < s_districts_per_wh; ++i) {
      district.d_w_id = w_id;
      district.d_id = i;
      d_keys[1] = i;
      uint64_t district_key = TPCCKeyGen::create_district_key(d_keys);
      storage_->PutRecord(1, district_key, (void*)&warehouse);
    }
  }

  // 3. Generate records for customer table
  uint32_t customer_keys[3];
  Customer customer;

  customer.c_id = -1;
  customer.c_d_id = -1;
  customer.c_w_id = -1;

  customer.c_discount = (rand() % 5001) / 10000.0;
  random.gen_rand_string(8, 16, customer.c_first);
  random.gen_rand_string(8, 16, customer.c_last);
  customer.c_credit_lim = 50000;
  customer.c_balance = -10;
  customer.c_ytd_payment = 10;
  customer.c_payment_cnt = 1;
  customer.c_delivery_cnt = 0;
  random.gen_rand_string(10, 20, customer.c_street_1);
  random.gen_rand_string(10, 20, customer.c_street_2);
  random.gen_rand_string(10, 20, customer.c_city);
  random.gen_rand_string(2, 2, customer.c_state);
  random.gen_rand_string(4, 4, customer.c_zip);
  for (int j = 4; j < 9; ++j) {
    customer.c_zip[j] = '1';
  }
  random.gen_rand_string(16, 16, customer.c_phone);
  customer.c_middle[0] = 'O';
  customer.c_middle[1] = 'E';
  customer.c_middle[2] = '\0';
  random.gen_rand_string(300, 500, customer.c_data);
  customer.c_since = 0;
  for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
    customer_keys[0] = w_id;
    for (uint32_t d_id = 0; d_id < s_districts_per_wh; ++d_id) {
      customer_keys[1] = d_id;
      for (uint32_t i = 0; i < s_customers_per_dist; ++i) {
        customer.c_id = i;
        customer.c_d_id = d_id;
        customer.c_w_id = w_id;

        if (i % 10 == 0) {		// 10% Bad Credit
          customer.c_credit[0] = 'B';
          customer.c_credit[1] = 'C';
          customer.c_credit[2] = '\0';
        } else {		// 90% Good Credit
          customer.c_credit[0] = 'G';
          customer.c_credit[1] = 'C';
          customer.c_credit[2] = '\0';
        }

        customer_keys[2] = i;
        uint64_t customer_key = TPCCKeyGen::create_customer_key(customer_keys);
        storage_->PutRecord(2, customer_key, (void*)&customer);
      }
    }
  }

  // 4. Generate records for item table
  Item item;
  item.i_id = -1;
  random.gen_rand_string(14, 24, item.i_name);
  item.i_price = (100 + (rand() % 9900)) / 100.0;
  int len = random.gen_rand_range(26, 50);
  random.gen_rand_string(len, len, item.i_data);
  item.i_im_id = 1 + (rand() % 10000);

  for (uint32_t i = 0; i < s_num_items; ++i) {
    if (i % 10 == 0) {
      int len = random.gen_rand_range(26, 50);
      int original_start = random.gen_rand_range(2, len-8);
      item.i_data[original_start] = 'O';
      item.i_data[original_start+1] = 'R';
      item.i_data[original_start+2] = 'I';
      item.i_data[original_start+3] = 'G';
      item.i_data[original_start+4] = 'I';
      item.i_data[original_start+5] = 'N';
      item.i_data[original_start+6] = 'A';
      item.i_data[original_start+7] = 'L';
    }
    item.i_id = i;
    uint64_t item_key = (uint64_t)i;
    storage_->PutRecord(7, item_key, (void*)&item);
  }

  // 5. Generate records for stock table
  Stock stock;
  uint32_t stock_keys[2];
  stock.s_i_id = -1;
  stock.s_w_id = -1;
  stock.s_quantity = 10 + rand() % 90;
  stock.s_ytd = 0;
  stock.s_order_cnt = 0;
  stock.s_remote_cnt = 0;
  random.gen_rand_string(len, len, stock.s_data);

  random.gen_rand_string(24, 24, stock.s_dist_01);
  random.gen_rand_string(24, 24, stock.s_dist_02);
  random.gen_rand_string(24, 24, stock.s_dist_03);
  random.gen_rand_string(24, 24, stock.s_dist_04);
  random.gen_rand_string(24, 24, stock.s_dist_05);
  random.gen_rand_string(24, 24, stock.s_dist_06);
  random.gen_rand_string(24, 24, stock.s_dist_07);
  random.gen_rand_string(24, 24, stock.s_dist_08);
  random.gen_rand_string(24, 24, stock.s_dist_09);
  random.gen_rand_string(24, 24, stock.s_dist_10);

  for (uint32_t w_id = 0; w_id < s_num_warehouses; ++w_id) {
    stock_keys[0] = w_id;
    for (uint32_t i = 0; i < s_num_items; ++i) {
      stock.s_i_id = i;
      stock.s_w_id = w_id;

      int len = random.gen_rand_range(26, 50);
      if (i%10 == 0) {
        int start_original = random.gen_rand_range(2, len-8);
        stock.s_data[start_original] = 'O';
        stock.s_data[start_original+1] = 'R';
        stock.s_data[start_original+2] = 'I';
        stock.s_data[start_original+3] = 'G';
        stock.s_data[start_original+4] = 'I';
        stock.s_data[start_original+5] = 'N';
        stock.s_data[start_original+6] = 'A';
        stock.s_data[start_original+7] = 'L';
      }

      stock_keys[1] = i;
      uint64_t stock_key = TPCCKeyGen::create_stock_key(stock_keys);
      storage_->PutRecord(8, stock_key, (void*)&stock);
    }
  }
}

//---------------------------------------------------------------

int TPCC::Execute2(LockUnit* lock_unit) const {
  return 0;
}
int TPCC::Rollback(LockUnit* lock_unit) const {
  return 0;
}

void TPCC::InitializeTable(uint32_t table_id) const {
}

