package com.grailsinaction

// Listing 15.15 A controller for pausing and resuming jobs programmatically
// http://localhost:8080/hubbub/jobAdmin/show
class JobAdminController {

    def jobManagerService

    def index = { redirect(action:'show') }
    def show = {
        def status = ""
        switch(params.operation) {
            case 'pause':
                jobManagerService.pauseJob("com.grailsinaction.ControllableJob", "myServices")
                status = "Paused Single Job"
                break
            case 'resume':
                jobManagerService.resumeJob("com.grailsinaction.ControllableJob", "myServices")
                status = "Resumed Single Job"
                break
            case 'pauseGroup':
                jobManagerService.pauseJobGroup("myServices")
                status = "Paused Job Group"
                break
            case 'resumeGroup':
                jobManagerService.resumeJobGroup("myServices")
                status = "Resumed Job Group"
                break
            case 'pauseAll':
                jobManagerService.pauseAll()
                status = "Paused All Jobs"
                break
            case 'resumeAll':
                jobManagerService.resumeAll()
                status = "Resumed All Jobs"
                break
        }
        return [ status: status ]
    }
}

