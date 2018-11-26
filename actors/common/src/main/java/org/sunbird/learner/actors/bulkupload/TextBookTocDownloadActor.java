package org.sunbird.learner.actors.bulkupload;

import org.sunbird.actor.core.BaseActor;
import org.sunbird.actor.router.ActorConfig;
import org.sunbird.common.request.Request;

import java.io.File;

/**
 * @author gauraw
 */

@ActorConfig(tasks = {"textbookTocDownload"}, asyncTasks = {})
public class TextBookTocDownloadActor extends BaseActor {

    @Override
    public void onReceive(Request request) throws Throwable {
        //File file =new File("/Users/kumar/downloaded_files/test.csv");
        File file =new File("test.csv");
        file.createNewFile();
        sender().tell(file, sender());
    }
}