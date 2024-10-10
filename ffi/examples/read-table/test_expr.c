#include "expression.h"

#define TEST_BUF_SIZE 4096

void read_expected_expression_tree(char* expected_buf, char* expected_path)
{
  FILE* data_file = fopen(expected_path, "r");
  if (NULL == data_file) {
    printf("Failed to open file\n");
    abort();
  }
  size_t offset = fread(expected_buf, sizeof(char), TEST_BUF_SIZE, data_file);
  if (0 == offset) {
    printf("Failed to read file\n");
    abort();
  }
  if (0 != fclose(data_file)) {
    printf("Error while closing file\n");
    abort();
  }
}
void get_expression_tree(ExpressionRef ref, char* out_buf, size_t buf_len)
{

  FILE* buf_stream = fmemopen(out_buf, buf_len, "w");
  if (NULL == buf_stream) {
    abort();
  }
  print_tree(buf_stream, ref, 0);
  fclose(buf_stream);
}
void test_kernel_expr(char* expected_path)
{
  SharedExpression* pred = get_kernel_expression();
  ExpressionRef ref = construct_predicate(pred);

  char out_buf[TEST_BUF_SIZE] = { 0 };
  char expected_buf[TEST_BUF_SIZE] = { 0 };

  read_expected_expression_tree(expected_buf, expected_path);
  get_expression_tree(ref, out_buf, TEST_BUF_SIZE);

  for (int i = 0; i < TEST_BUF_SIZE; i++) {
    assert(out_buf[i] == expected_buf[i]);
  }

  free_expression(ref);
  free_kernel_predicate(pred);
}

int main(int argc, char* argv[])
{
  if (argc < 2) {
    printf("Usage: %s expected results path\n", argv[0]);
    return -1;
  }

  char* expected_path = argv[1];
  test_kernel_expr(expected_path);
  printf("Success!\n");
}
