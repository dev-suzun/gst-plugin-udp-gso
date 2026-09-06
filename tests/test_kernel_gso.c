/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif

#ifndef SOL_UDP
#define SOL_UDP IPPROTO_UDP
#endif

#define SEGMENT_SIZE 1200
#define SEGMENT_COUNT 8

static int
fail (const char *message)
{
  perror (message);
  return EXIT_FAILURE;
}

int
main (void)
{
  struct sockaddr_in address = { 0 };
  socklen_t address_length = sizeof (address);
  struct timeval timeout = { .tv_sec = 2, .tv_usec = 0 };
  unsigned char payload[SEGMENT_SIZE * SEGMENT_COUNT];
  unsigned char received[SEGMENT_SIZE + 1];
  char control[CMSG_SPACE (sizeof (uint16_t))] = { 0 };
  struct iovec vector = { .iov_base = payload, .iov_len = sizeof (payload) };
  struct msghdr message = { 0 };
  struct cmsghdr *control_message;
  uint16_t segment_size = SEGMENT_SIZE;
  int receiver = -1;
  int sender = -1;
  int result = EXIT_FAILURE;
  int i;

  receiver = socket (AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (receiver < 0)
    return fail ("receiver socket");

  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
  address.sin_port = 0;
  if (bind (receiver, (struct sockaddr *) &address, sizeof (address)) < 0) {
    fail ("bind");
    goto out;
  }
  if (getsockname (receiver, (struct sockaddr *) &address,
          &address_length) < 0) {
    fail ("getsockname");
    goto out;
  }
  if (setsockopt (receiver, SOL_SOCKET, SO_RCVTIMEO, &timeout,
          sizeof (timeout)) < 0) {
    fail ("SO_RCVTIMEO");
    goto out;
  }

  sender = socket (AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (sender < 0) {
    fail ("sender socket");
    goto out;
  }
  if (connect (sender, (struct sockaddr *) &address, sizeof (address)) < 0) {
    fail ("connect");
    goto out;
  }

  for (i = 0; i < SEGMENT_COUNT; i++)
    memset (payload + (i * SEGMENT_SIZE), i + 1, SEGMENT_SIZE);

  message.msg_iov = &vector;
  message.msg_iovlen = 1;
  message.msg_control = control;
  message.msg_controllen = sizeof (control);
  control_message = CMSG_FIRSTHDR (&message);
  control_message->cmsg_level = SOL_UDP;
  control_message->cmsg_type = UDP_SEGMENT;
  control_message->cmsg_len = CMSG_LEN (sizeof (segment_size));
  memcpy (CMSG_DATA (control_message), &segment_size, sizeof (segment_size));

  if (sendmsg (sender, &message, 0) != (ssize_t) sizeof (payload)) {
    if (errno == ENOPROTOOPT || errno == EOPNOTSUPP) {
      fprintf (stderr, "SKIP: UDP_SEGMENT is not supported by this kernel\n");
      result = 77;
      goto out;
    }
    fail ("UDP_SEGMENT sendmsg");
    goto out;
  }

  for (i = 0; i < SEGMENT_COUNT; i++) {
    ssize_t length = recv (receiver, received, sizeof (received), 0);
    size_t j;

    if (length != SEGMENT_SIZE) {
      fprintf (stderr, "unexpected datagram length: %zd\n", length);
      goto out;
    }
    for (j = 0; j < SEGMENT_SIZE; j++) {
      if (received[j] != (unsigned char) (i + 1)) {
        fprintf (stderr, "payload mismatch in datagram %d at byte %zu\n", i, j);
        goto out;
      }
    }
  }

  printf ("UDP_SEGMENT delivered %d datagrams of %d bytes\n",
      SEGMENT_COUNT, SEGMENT_SIZE);
  result = EXIT_SUCCESS;

out:
  if (sender >= 0)
    close (sender);
  if (receiver >= 0)
    close (receiver);
  return result;
}

