int ch1(int key, int num_buckets) {
  random.seed(key);
  int b = 0; // b will track ch(key, j +1) .
  for (int j = 1; j < num_buckets; j++) {
    if (random.next() < 1.0 / (j + 1))
      b = j;
  }
  return b;
}

int ch(int key, int num_buckets) {
  random.seed(key);
  int b = -1; //  bucket number before the previous jump
  int j = 0;  // bucket number before the current jump
  while (j < num_buckets) {
    b = j;
    double r = random.next(); //  0<r<1.0
    j = floor((b + 1) / r);
  }
  return b;
}