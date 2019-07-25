package org.sunbird.user.actors;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.models.response.Response;
import org.sunbird.common.request.Request;

import java.util.Collections;

@ActorConfig(
        tasks = {"freeUpUser"},
        asyncTasks = {}
)
public class UserFreedActor extends BaseActor {
    @Override
    public void onReceive(Request request) throws Throwable {
        Response response = new Response();
        response.getResult().put("message", "success");
        response.put("data recieved", Collections.singleton(request.getRequest()));
        sender().tell(response, self());
    }
}