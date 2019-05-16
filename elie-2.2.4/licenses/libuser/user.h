/*
 * Copyright (C) 2000-2002, 2004 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Library General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
 */

#ifndef libuser_user_h
#define libuser_user_h

#include <sys/types.h>
#include <glib.h>
#include "config.h"
#include "entity.h"
#include "error.h"
#include "fs.h"
#include "prompt.h"

G_BEGIN_DECLS

/**
 * LU_VALUE_INVALID_ID:
 *
 * An #id_t value that matches no valid user or group ID.
 */
/* (id_t)-1 is used by setreuid() to indicate "not a valid ID", so it should be
   safe to use for error indication. */
#define LU_VALUE_INVALID_ID ((id_t)-1)

/**
 * lu_context:
 *
 * An opaque structure manipulated by the library, containing caller-related
 * state (to allow several independent callers in a single process).
 */
struct lu_context;
#ifndef LU_DISABLE_DEPRECATED
/**
 * lu_context_t:
 *
 * An alias for struct #lu_context.
 * Deprecated: 0.57.3: Use struct #lu_context directly.
 */
typedef struct lu_context lu_context_t;
#endif

/**
 * lu_entity_type:
 * @lu_user: An user.
 * @lu_group: A group of users.
 *
 * Defines whether we are talking about an user or a group in cases where it is
 * ambiguous.
 */
enum lu_entity_type {
	/*< private >*/
	lu_invalid,
	/*< public >*/
	lu_user,
	lu_group,
};
#ifndef __GTK_DOC_IGNORE__
#ifndef LU_DISABLE_DEPRECATED
typedef enum lu_entity_type lu_entity_type_t;
#endif
#endif

char *lu_value_strdup(const GValue *value);
int lu_values_equal(const GValue *a, const GValue *b);
void lu_value_init_set_id(GValue *value, id_t id);
id_t lu_value_get_id(const GValue *value);
gboolean lu_value_init_set_attr_from_string(GValue *value, const char *attr,
					    const char *string,
					    struct lu_error **error);

struct lu_context *lu_start(const char *authname, enum lu_entity_type auth_type,
			    const char *modules, const char *create_modules,
			    lu_prompt_fn *prompter, gpointer callback_data,
			    struct lu_error **error);
void lu_end(struct lu_context *context);

void lu_set_prompter(struct lu_context *context,
		     lu_prompt_fn *prompter, gpointer callback_data);
void lu_get_prompter(struct lu_context *context,
		     lu_prompt_fn ** prompter, gpointer *callback_data);

gboolean lu_set_modules(struct lu_context *context,
			const char *list, struct lu_error **error);
const char *lu_get_modules(struct lu_context *context);
gboolean lu_uses_elevated_privileges (struct lu_context *context);

gboolean lu_user_default(struct lu_context *ctx, const char *name,
			 gboolean system_account, struct lu_ent *ent);
gboolean lu_group_default(struct lu_context *ctx, const char *name,
			  gboolean system_account, struct lu_ent *ent);

gboolean lu_user_lookup_name(struct lu_context *context,
			     const char *name, struct lu_ent *ent,
			     struct lu_error **error);
gboolean lu_group_lookup_name(struct lu_context *context,
			      const char *name, struct lu_ent *ent,
			      struct lu_error **error);
gboolean lu_user_lookup_id(struct lu_context *context, uid_t uid,
			   struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_lookup_id(struct lu_context *context, gid_t gid,
			    struct lu_ent *ent, struct lu_error **error);
gboolean lu_user_add(struct lu_context *context,
		     struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_add(struct lu_context *context,
		      struct lu_ent *ent, struct lu_error **error);
gboolean lu_user_modify(struct lu_context *context,
			struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_modify(struct lu_context *context,
			 struct lu_ent *ent, struct lu_error **error);
gboolean lu_user_delete(struct lu_context *context,
			struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_delete(struct lu_context *context,
			 struct lu_ent *ent, struct lu_error **error);

gboolean lu_user_lock(struct lu_context *context,
		      struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_lock(struct lu_context *context,
		       struct lu_ent *ent, struct lu_error **error);
gboolean lu_user_unlock(struct lu_context *context,
			struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_unlock(struct lu_context *context,
			 struct lu_ent *ent, struct lu_error **error);
gboolean lu_user_unlock_nonempty(struct lu_context *context, struct lu_ent *ent,
				 struct lu_error **error);
gboolean lu_group_unlock_nonempty(struct lu_context *context,
				  struct lu_ent *ent, struct lu_error **error);

gboolean lu_user_islocked(struct lu_context *context,
			  struct lu_ent *ent, struct lu_error **error);
gboolean lu_group_islocked(struct lu_context *context,
			   struct lu_ent *ent, struct lu_error **error);

gboolean lu_user_setpass(struct lu_context *context,
			 struct lu_ent *ent, const char *newpass,
			 gboolean crypted,
			 struct lu_error **error);
gboolean lu_group_setpass(struct lu_context *context,
			  struct lu_ent *ent, const char *newpass,
			  gboolean crypted,
			  struct lu_error **error);
gboolean lu_user_removepass(struct lu_context *context,
			    struct lu_ent *ent,
			    struct lu_error **error);
gboolean lu_group_removepass(struct lu_context *context,
			     struct lu_ent *ent,
			     struct lu_error **error);

GValueArray *lu_users_enumerate(struct lu_context *context,
				const char *pattern,
				struct lu_error **error);
GValueArray *lu_groups_enumerate(struct lu_context *context,
				 const char *pattern,
				 struct lu_error **error);
GValueArray *lu_users_enumerate_by_group(struct lu_context *context,
					 const char *group,
					 struct lu_error **error);
GValueArray *lu_groups_enumerate_by_user(struct lu_context *context,
					 const char *user,
					 struct lu_error **error);

GPtrArray *lu_users_enumerate_full(struct lu_context *context,
			           const char *pattern,
			           struct lu_error **error);
GPtrArray *lu_groups_enumerate_full(struct lu_context *context,
			            const char *pattern,
			            struct lu_error **error);
GPtrArray *lu_users_enumerate_by_group_full(struct lu_context *context,
					    const char *group,
					    struct lu_error **error);
GPtrArray *lu_groups_enumerate_by_user_full(struct lu_context *context,
					    const char *user,
					    struct lu_error **error);

G_END_DECLS
#endif
