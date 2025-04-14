package com.grailsinaction

class PhotoUploadCommand {
    byte[] photo
    String loginId
}

// Listing 7.14 Handling image uploading via a command object
class ImageController {

    def upload(PhotoUploadCommand puc) {
        def user = User.findByLoginId(puc.loginId)
        user.profile.photo = puc.photo
        redirect controller: "user", action: "profile", id: puc.loginId
    }

    def rawUpload() {
        // a Spring MultipartFile
        def mhsr = request.getFile('photo')
        if (!mhsr?.empty && mhrs.size < 1024 * 200) { // 200kb
            mhsr.transferTo(
                    new File('/hubbub/images/${request.userId}/mugshot.gif'))

        }
    }

    def form() {
        // pass through to upload form
        [userList: User.list()]
    }

    def view = {
        // path through to "view photo" page
    }

    // Listing 7.16 Sending image data to the browser
    def renderImage(String id) {
        def user = User.findByLoginId(id)
        if (user?.profile?.photo) {
            response.setContentLength(user.profile.photo.size())
            response.outputStream.write(user.profile.photo)
        } else {
            response.sendError(404)
        }
    }

    def tiny = {
        if (params.id) {
            def image = imageService.getUserTinyThumbnail(params.id)
            response.setContentLength(image.length)
            response.getOutputStream().write(image)
        } else {
            response.sendError(404)
        }

    }

}
