package org.sunbird.user;

import org.sunbird.user.actors.UserManagementActor;

/**
 * 
 * @author Mahesh Kumar Gangula
 *
 */

public class UserActorRegistry {

	public UserActorRegistry() {
		UserManagementActor.init();
	}
}
