#include <stdio.h>
#include <stdlib.h>
#include <liburing.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

namespace {

}

void TestUringSupport() {
  struct io_uring ring;
  int ret;
  ret = io_uring_queue_init(32, &ring, 0);
  if (ret < 0) {
      perror("io_uring_queue_init");
      return;
  }
  printf("io_uring is supported on this system.\n");
  io_uring_queue_exit(&ring);
}

#define FILE_PATH "example.txt"


int BasicReadWrite() {
    struct io_uring ring;
    int ret;

    // 初始化io_uring
    ret = io_uring_queue_init(32, &ring, 0);
    if (ret < 0) {
        perror("io_uring_queue_init");
        return 1;
    }

    // 打开文件
    int file_fd = open(FILE_PATH, O_RDWR | O_CREAT, 0644);
    if (file_fd == -1) {
        perror("open");
        return 1;
    }

    // 准备写入数据
    const char* data_to_write = "Hello, io_uring!";
    size_t data_size = strlen(data_to_write);

    // 创建一个io_uring的写操作
    struct io_uring_sqe* write_sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(write_sqe, file_fd, data_to_write, data_size, 0);

    // 提交写操作
    io_uring_submit(&ring);

    // 创建一个io_uring的读操作
    char buffer[1024];
    struct io_uring_sqe* read_sqe = io_uring_get_sqe(&ring);
    io_uring_prep_read(read_sqe, file_fd, buffer, sizeof(buffer), 0);

    // 提交读操作
    io_uring_submit(&ring);

    // 等待操作完成
    struct io_uring_cqe* cqe;
    io_uring_wait_cqe(&ring, &cqe);

    // 处理写操作的结果
    if (cqe->res < 0) {
        perror("write");
    } else {
        printf("Write operation completed successfully.\n");
    }
    io_uring_cqe_seen(&ring, cqe);

    // 等待读操作完成
    io_uring_wait_cqe(&ring, &cqe);

    // 处理读操作的结果
    if (cqe->res < 0) {
        perror("read");
    } else {
        buffer[cqe->res] = '\0'; // 在读取的数据末尾添加终止符
        printf("Read operation completed successfully. Data: %s\n", buffer);
    }
    io_uring_cqe_seen(&ring, cqe);

    // 关闭文件和io_uring
    close(file_fd);
    io_uring_queue_exit(&ring);

    return 0;
}


int main() {
    BasicReadWrite();
    return 0;
}
