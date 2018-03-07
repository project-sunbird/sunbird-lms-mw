package org.sunbird.actor.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.sunbird.actor.router.BackgroundRequestRouter;

public abstract class ActorRegistry {

	protected Map<Class<?>, List<String>> actors = new HashMap<>();
	protected Map<Class<?>, List<String>> bgActors = new HashMap<>();
	
	public void initActors() {
		
	}
	
	public void initBackgroundActors() {
		System.out.println("initBackgroundActors length: "+ bgActors.size());
		try {
			for (Class<?> clazz : bgActors.keySet()) {
				BackgroundRequestRouter.registerActor(clazz, bgActors.get(clazz));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
