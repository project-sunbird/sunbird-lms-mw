package org.sunbird.learner.actors;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sunbird.common.exception.ProjectCommonException;
import org.sunbird.common.models.util.ActorOperations;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.request.Request;
import org.sunbird.common.responsecode.ResponseCode;
import org.sunbird.learner.actors.assessment.AssessmentItemActor;
import org.sunbird.learner.actors.recommend.RecommendorActor;
import org.sunbird.learner.actors.search.CourseSearchActor;
import org.sunbird.learner.actors.search.SearchHandlerActor;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.routing.FromConfig;
import akka.util.Timeout;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * @author  arvind .
 * Class to initialize and select the appropriate actor on the basis of message type .
 */
public class RequestRouterActor extends UntypedAbstractActor {

    private ActorRef courseEnrollmentActorRouter;
    private ActorRef learnerStateActorRouter;
    private ActorRef learnerStateUpdateActorRouter;
    private ActorRef userManagementRouter;
    private ActorRef courseManagementRouter;
    private ActorRef pageManagementRouter;
    private ActorRef organisationManagementRouter;
    private ActorRef recommendorActorRouter;
    public static ActorRef backgroundJobManager;
    private ActorRef courseSearchActorRouter;
    private ActorRef assessmentItemActor;
    private ActorRef searchHandlerActor; 
    private ExecutionContext ec;
    Map<String, ActorRef> routerMap = new HashMap<>();
    private static final int WAIT_TIME_VALUE = 9;
    private static final String COURSE_ENROLLMENT_ROUTER = "courseEnrollmentRouter";
    private static final String LEARNER_ACTOR_ROUTER = "learnerActorRouter";
    private static final String LEARNER_STATE_ROUTER = "learnerStateRouter";
    private static final String USER_MANAGEMENT_ROUTER = "userManagementRouter";
    private static final String COURSE_MANAGEMENT_ROUTER = "courseManagementRouter";
    private static final String PAGE_MANAGEMENT_ROUTER = "pageManagementRouter";
    private static final String ORGANISATION_MANAGEMENT_ROUTER = "organisationManagementRouter";
    private static final String BkJOB = "backgroundJobManager";
    private static final String COURSE_SEARCH_ACTOR_ROUTER = "courseSearchActorRouter";
    private static final String ASSESSMENT_ITEM_ACTOR_ROUTER = "assessmentItemActor";
    private static final String RECOMMENDOR_ACTOR_ROUTER = "recommendorActorRouter";
    private static final String SEARCH_HANDLER_ACTOR_ROUTER = "searchHandlerActor";
    /**
     * constructor to initialize router actor with child actor pool
     */
    public RequestRouterActor() {
        courseEnrollmentActorRouter =
                getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseEnrollmentActor.class)),
                        COURSE_ENROLLMENT_ROUTER);
        learnerStateActorRouter =
                getContext().actorOf(FromConfig.getInstance().props(Props.create(LearnerStateActor.class)),
                        LEARNER_ACTOR_ROUTER);
        learnerStateUpdateActorRouter =
                getContext().actorOf(FromConfig.getInstance().props(Props.create(LearnerStateUpdateActor.class)),
                        LEARNER_STATE_ROUTER);
        userManagementRouter = getContext().actorOf(FromConfig.getInstance().props(Props.create(UserManagementActor.class)),
                        USER_MANAGEMENT_ROUTER);
        courseManagementRouter = getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseManagementActor.class)),
                COURSE_MANAGEMENT_ROUTER);
        pageManagementRouter = getContext().actorOf(FromConfig.getInstance().props(Props.create(PageManagementActor.class)),
                PAGE_MANAGEMENT_ROUTER);
        organisationManagementRouter = getContext().actorOf(FromConfig.getInstance().props(Props.create(OrganisationManagementActor.class)),
                ORGANISATION_MANAGEMENT_ROUTER);
        backgroundJobManager = getContext().actorOf(FromConfig.getInstance().props(Props.create(BackgroundJobManager.class)),BkJOB);
        courseSearchActorRouter = getContext().actorOf(FromConfig.getInstance().props(Props.create(CourseSearchActor.class)),
                COURSE_SEARCH_ACTOR_ROUTER);
        assessmentItemActor = getContext().actorOf(FromConfig.getInstance().props(Props.create(AssessmentItemActor.class)),
                ASSESSMENT_ITEM_ACTOR_ROUTER);
        recommendorActorRouter=getContext().actorOf(FromConfig.getInstance().props(Props.create(RecommendorActor.class)),
                RECOMMENDOR_ACTOR_ROUTER);
        searchHandlerActor=getContext().actorOf(FromConfig.getInstance().props(Props.create(SearchHandlerActor.class)),
            SEARCH_HANDLER_ACTOR_ROUTER);
        ec = getContext().dispatcher();
        initializeRouterMap();
    }

    /**
     * Initialize the map with operation as key and corresponding router as value.
     */
    private void initializeRouterMap() {
        routerMap.put(ActorOperations.ENROLL_COURSE.getValue(), courseEnrollmentActorRouter);
        routerMap.put(ActorOperations.GET_COURSE.getValue(), learnerStateActorRouter);
        routerMap.put(ActorOperations.GET_CONTENT.getValue(), learnerStateActorRouter);
        routerMap.put(ActorOperations.ADD_CONTENT.getValue(), learnerStateUpdateActorRouter);

        routerMap.put(ActorOperations.CREATE_COURSE.getValue(), courseManagementRouter);
        routerMap.put(ActorOperations.UPDATE_COURSE.getValue(), courseManagementRouter);
        routerMap.put(ActorOperations.PUBLISH_COURSE.getValue(), courseManagementRouter);
        routerMap.put(ActorOperations.DELETE_COURSE.getValue(), courseManagementRouter);

        routerMap.put(ActorOperations.CREATE_USER.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.UPDATE_USER.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.LOGIN.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.LOGOUT.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.CHANGE_PASSWORD.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.GET_PROFILE.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.GET_ROLES.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.GET_USER_DETAILS_BY_LOGINID.getValue(), userManagementRouter);
        routerMap.put(ActorOperations.DOWNLOAD_USERS.getValue(), userManagementRouter);
        
        routerMap.put(ActorOperations.CREATE_PAGE.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.UPDATE_PAGE.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.GET_PAGE_DATA.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.GET_PAGE_SETTINGS.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.GET_PAGE_SETTING.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.CREATE_SECTION.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.UPDATE_SECTION.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.GET_SECTION.getValue(), pageManagementRouter);
        routerMap.put(ActorOperations.GET_ALL_SECTION.getValue(), pageManagementRouter);
        
        routerMap.put(ActorOperations.CREATE_ORG.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.APPROVE_ORG.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.UPDATE_ORG.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.UPDATE_ORG_STATUS.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.GET_ORG_DETAILS.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.ADD_MEMBER_ORGANISATION.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.REMOVE_MEMBER_ORGANISATION.getValue(), organisationManagementRouter);
        
        routerMap.put(ActorOperations.SEARCH_COURSE.getValue(), courseSearchActorRouter);
        routerMap.put(ActorOperations.GET_COURSE_BY_ID.getValue(), courseSearchActorRouter);
        
        routerMap.put(ActorOperations.GET_ASSESSMENT.getValue(), assessmentItemActor);
        routerMap.put(ActorOperations.SAVE_ASSESSMENT.getValue(), assessmentItemActor);
        routerMap.put(ActorOperations.GET_RECOMMENDED_COURSES.getValue(), recommendorActorRouter);
        routerMap.put(ActorOperations.APPROVE_USER_ORGANISATION.getValue() , organisationManagementRouter);
        routerMap.put(ActorOperations.JOIN_USER_ORGANISATION.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.COMPOSITE_SEARCH.getValue(), searchHandlerActor);
        routerMap.put(ActorOperations.REJECT_USER_ORGANISATION.getValue(), organisationManagementRouter);
        routerMap.put(ActorOperations.DOWNLOAD_ORGS.getValue(), organisationManagementRouter);
        
    }


    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Request) {
        	System.out.println("Received actor message....");
            ProjectLogger.log("Actor selector onReceive called");
            Request actorMessage = (Request) message;
            org.sunbird.common.request.ExecutionContext.setRequestId(actorMessage.getRequestId());
            ActorRef ref = routerMap.get(actorMessage.getOperation());
            if (null != ref) {
                route(ref, actorMessage);
            } else {
                ProjectLogger.log("UNSUPPORTED OPERATION TYPE");
                ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidOperationName.getErrorCode(), ResponseCode.invalidOperationName.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                sender().tell(exception, ActorRef.noSender());
            }
        } else {
            ProjectLogger.log("UNSUPPORTED MESSAGE");
            ProjectCommonException exception = new ProjectCommonException(ResponseCode.invalidRequestData.getErrorCode(), ResponseCode.invalidRequestData.getErrorMessage(), ResponseCode.SERVER_ERROR.getResponseCode());
            sender().tell(exception, ActorRef.noSender());
        }

    }

    /**
     * method will route the message to corresponding router pass into the argument .
     *
     * @param router
     * @param message
     * @return boolean
     */
    private boolean route(ActorRef router, Request message) {

        Timeout timeout = new Timeout(Duration.create(WAIT_TIME_VALUE, TimeUnit.SECONDS));
        Future<Object> future = Patterns.ask(router, message, timeout);
        ActorRef parent = sender();
        future.onComplete(new OnComplete<Object>() {
            @Override
            public void onComplete(Throwable failure, Object result) {
                if (failure != null) {
                    //We got a failure, handle it here
                    ProjectLogger.log(failure.getMessage(), failure);
                    if(failure instanceof ProjectCommonException){
                        parent.tell(failure, ActorRef.noSender());
                    }else{
                        ProjectCommonException exception = new ProjectCommonException(ResponseCode.internalError.getErrorCode(), ResponseCode.internalError.getErrorMessage(), ResponseCode.CLIENT_ERROR.getResponseCode());
                        parent.tell(exception, ActorRef.noSender());
                    }
                } else {
                    ProjectLogger.log("PARENT RESULT IS " + result);
                    // We got a result, handle it
                    parent.tell(result, ActorRef.noSender());
                }
            }
        }, ec);
        return true;
    }
}