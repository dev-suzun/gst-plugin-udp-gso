/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <linux/net_tstamp.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#ifndef SCM_TXTIME
#define SCM_TXTIME SO_TXTIME
#endif

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
  struct sock_txtime socket_txtime = {
    .clockid = CLOCK_MONOTONIC,
    .flags = 0,
  };
  struct timespec now;
  const unsigned char payload[] = "txtime";
  unsigned char received[sizeof (payload)] = { 0 };
  char control[CMSG_SPACE (sizeof (uint64_t))] = { 0 };
  struct iovec vector = {
    .iov_base = (void *) payload,
    .iov_len = sizeof (payload),
  };
  struct msghdr message = { 0 };
  struct cmsghdr *control_message;
  uint64_t txtime_ns;
  int receiver = -1;
  int sender = -1;
  int result = EXIT_FAILURE;

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
  if (setsockopt (sender, SOL_SOCKET, SO_TXTIME, &socket_txtime,
          sizeof (socket_txtime)) < 0) {
    if (errno == ENOPROTOOPT || errno == EOPNOTSUPP || errno == EPERM) {
      fprintf (stderr, "SKIP: SO_TXTIME is unavailable: %s\n",
          strerror (errno));
      result = 77;
      goto out;
    }
    fail ("SO_TXTIME");
    goto out;
  }

  if (clock_gettime (CLOCK_MONOTONIC, &now) < 0) {
    fail ("clock_gettime");
    goto out;
  }
  txtime_ns = ((uint64_t) now.tv_sec * UINT64_C (1000000000)) + now.tv_nsec +
      UINT64_C (1000000);

  message.msg_iov = &vector;
  message.msg_iovlen = 1;
  message.msg_control = control;
  message.msg_controllen = sizeof (control);
  control_message = CMSG_FIRSTHDR (&message);
  control_message->cmsg_level = SOL_SOCKET;
  control_message->cmsg_type = SCM_TXTIME;
  control_message->cmsg_len = CMSG_LEN (sizeof (txtime_ns));
  memcpy (CMSG_DATA (control_message), &txtime_ns, sizeof (txtime_ns));

  if (sendmsg (sender, &message, 0) != (ssize_t) sizeof (payload)) {
    if (errno == EINVAL || errno == ENOPROTOOPT || errno == EOPNOTSUPP ||
        errno == EPERM) {
      fprintf (stderr,
          "SKIP: SCM_TXTIME needs a compatible queue discipline: %s\n",
          strerror (errno));
      result = 77;
      goto out;
    }
    fail ("SCM_TXTIME sendmsg");
    goto out;
  }

  if (recv (receiver, received, sizeof (received), 0) !=
      (ssize_t) sizeof (payload)) {
    fail ("recv");
    goto out;
  }
  if (memcmp (payload, received, sizeof (payload)) != 0) {
    fprintf (stderr, "received payload does not match\n");
    goto out;
  }

  printf ("SO_TXTIME/SCM_TXTIME delivered the test datagram\n");
  result = EXIT_SUCCESS;

out:
  if (sender >= 0)
    close (sender);
  if (receiver >= 0)
    close (receiver);
  return result;
}

