(function () {
  var videos = document.querySelectorAll("video[data-demo]");
  if (!videos.length || !("IntersectionObserver" in window)) return;
  if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (entry) {
      var video = entry.target;
      if (entry.isIntersecting) {
        var playing = video.play();
        if (playing && playing.catch) playing.catch(function () {});
      } else {
        video.pause();
      }
    });
  }, { rootMargin: "200px 0px" });
  videos.forEach(function (video) {
    video.removeAttribute("controls");
    observer.observe(video);
  });
})();
