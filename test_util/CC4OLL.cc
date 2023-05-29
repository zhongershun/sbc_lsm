#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <pthread.h>

using namespace std;

struct Node {
    int data_;
    Node *next_;
    mutex mutex_;
    Node():next_(nullptr){};
    Node(int data):data_(data), next_(nullptr){};
};

void Add(int data, Node *head) {
    // 空指针不能加锁 记得异常处理
    head->mutex_.lock();
    Node *pred = head;
    Node *curr = head->next_;
    if (curr != nullptr)
        curr->mutex_.lock();
    while (curr != nullptr && curr->data_ < data) {
        pred->mutex_.unlock();
        pred = curr;
        curr = curr->next_;
        if (curr != nullptr)
            curr->mutex_.lock();
    }
    // 分三种情况判断：已经存在、已经位于队尾、正常插入（位于队中）
    if (curr == nullptr) {
        Node *temp = new Node(data);
        pred->next_ = temp;
        cout << data << " add ok ";
    } else if (curr->data_ == data) {
        cout << "Already exist" << endl;
    } else {
        Node *temp = new Node(data);
        temp->next_ = curr;
        pred->next_ = temp;
        cout << data << " add ok ";
    }
    cout << endl;
    if (curr != nullptr)
        curr->mutex_.unlock();
    pred->mutex_.unlock();
}

void Add_nolock(int data, Node *head) {
    Node *pred = head;
    Node *curr = head->next_;
    while (curr != nullptr && curr->data_ < data) {
        pred = curr;
        curr = curr->next_;
    }
    if (curr == nullptr) {
        Node *temp = new Node(data);
        pred->next_ = temp;
        cout << data << " add ok ";
    } else if (curr->data_ == data) {
        cout << "Already exist" << endl;
    } else {
        Node *temp = new Node(data);
        temp->next_ = curr;
        pred->next_ = temp;
        cout << data << " add ok ";
    }
    cout << endl;
}

void Contain(int data, Node *head) {
    head->mutex_.lock();
    Node *pred = head;
    Node *curr = head->next_;
    if (curr != nullptr)
        curr->mutex_.lock();
    while (curr != nullptr && curr->data_ < data) {
        pred->mutex_.unlock();
        pred = curr;
        curr = curr->next_;
        if (curr != nullptr)
            curr->mutex_.lock();
    }
    if (curr != nullptr && curr->data_ == data) {
        cout << "Exist" << endl;
    } else {
        cout << "Not exist" << endl;
    }
    if (curr != nullptr)
        curr->mutex_.unlock();
    pred->mutex_.unlock();
}

void Remove(int data, Node *head) {
    head->mutex_.lock();
    Node *pred = head;
    Node *curr = head->next_;
    if (curr != nullptr)
        curr->mutex_.lock();
    while (curr != nullptr && curr->data_ < data) {
        pred->mutex_.unlock();
        pred = curr;
        curr = curr->next_;
        if (curr != nullptr)
            curr->mutex_.lock();
    }
    if (curr != nullptr && curr->data_ == data) {
        pred->next_= curr->next_;
        delete curr;
        cout << data << " delete ok ";
    } else {
        cout << "Not exist" << endl;
    }
    cout << endl;
    if (curr != nullptr)
        curr->mutex_.unlock();
    pred->mutex_.unlock();
}

void Print(Node *head) {
    // 只需要锁头节点
    printf("****************************************************************\n");
    head->mutex_.lock();
    Node *curr = head->next_;
    while (curr != nullptr) {
        cout << curr->data_ << " ";
        curr = curr->next_;
    }
    cout << endl;
    head->mutex_.unlock();
}

int main() {
    Node *head = new Node();
    // Test Case 1
//    vector<thread> addthreads;
//    for (int i = 0; i < 100; i++)
//        addthreads.emplace_back(Add_nolock, i, head);
//    for (auto &thread1 : addthreads)
//        thread1.join();
//    Print(head);
    // Test Case 2
//    vector<thread> addthreads;
//    for (int i = 0; i < 100; i++)
//        addthreads.emplace_back(Add, i, head);
//    for (auto &thread1 : addthreads)
//        thread1.join();
//    Print(head);
//
//    vector<thread> remthreads;
//    for (int i = 0; i < 100; i++)
//        remthreads.emplace_back(Remove, i, head);
//    for (auto &thread2 : remthreads)
//        thread2.join();
//    Print(head);

    // Test case 3
    vector<thread> addthreads;
    for (int i = 0; i < 100; i = i + 2)
        addthreads.emplace_back(Add, i, head);
    for (auto &thread1 : addthreads)
        thread1.join();
    Print(head);
    vector<thread> addremthreads;
    for (int i = 0; i < 100; i = i + 2) {
        addremthreads.emplace_back(Add, i+1, head);
        addremthreads.emplace_back(Remove, i, head);
    }
    for (auto &thread3 : addremthreads)
        thread3.join();
    Print(head);
    // Test case 4
//    vector<thread> addthreads;
//    for (int i = 0; i < 100; i++)
//        addthreads.emplace_back(Add, i, head);
//    for (auto &thread1 : addthreads)
//        thread1.join();
//    Print(head);
//    vector<thread> conremthreads;
//    for (int i = 0; i < 97; i = i + 2) {
//        conremthreads.emplace_back(Contain, i+3, head);
//        conremthreads.emplace_back(Remove, i, head);
//    }
//    for (auto &thread4 : conremthreads)
//        thread4.join();
//    Print(head);
    delete head;
    return 0;
}
