/* Copyright (C) 2002 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <rpc/rpc.h>
#include <sys/socket.h>

#include "common.h"
#include "pot.h"

static int svc_socket (u_long number, int type, int protocol, int port, int reuse)
{
	struct sockaddr_in addr;
	char rpcdata [1024], servdata [1024];
	struct rpcent rpcbuf, *rpcp = NULL;
	struct servent servbuf, *servp = NULL;
	int sock, ret;
	const char *proto = protocol == IPPROTO_TCP ? "tcp" : "udp";

	if ((sock = socket (AF_INET, type, protocol)) < 0) {
		errstr(_("Cannot create socket: %s\n"), strerror(errno));
		return -1;
	}

	if (reuse) {
		ret = 1;
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(ret)) < 0) {
			errstr(_("Cannot set socket options: %s\n"), strerror(errno));
			return -1;
		}
	}

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;

	if (!port) {
		ret = getrpcbynumber_r(number, &rpcbuf, rpcdata, sizeof(rpcdata), &rpcp);
		if (ret == 0 && rpcp != NULL) {
			/* First try name */
			ret = getservbyname_r(rpcp->r_name, proto, &servbuf, servdata,
			                       sizeof servdata, &servp);
			if ((ret != 0 || servp == NULL) && rpcp->r_aliases) {
				const char **a;

				/* Then we try aliases.	*/
				for (a = (const char **) rpcp->r_aliases; *a != NULL; a++) {
					ret = getservbyname_r(*a, proto, &servbuf, servdata,
							 sizeof servdata, &servp);
					if (ret == 0 && servp != NULL)
						break;
				}
			}
			if (ret == 0 && servp != NULL)
				port = servp->s_port;
		}
	}
	else
		port = htons(port);

	if (port) {
		addr.sin_port = port;
		if (bind(sock, (struct sockaddr *) &addr, sizeof(struct sockaddr_in)) < 0) {
			errstr(_("Cannot bind to given address: %s\n"), strerror(errno));
			close (sock);
			return -1;
		}
	}
	else {
		/* Service not found? */
		close(sock);
		return -1;
	}

	return sock;
}

/*
 * Create and bind a TCP socket based on program number
 */
int svctcp_socket(u_long number, int port, int reuse)
{
	return svc_socket(number, SOCK_STREAM, IPPROTO_TCP, port, reuse);
}

/*
 * Create and bind a UDP socket based on program number
 */
int svcudp_socket(u_long number, int port, int reuse)
{
	return svc_socket(number, SOCK_DGRAM, IPPROTO_UDP, port, reuse);
}
