; Copyright 2010 WebMapReduce Developers
; Licensed under the Apache License, Version 2.0 (the "License");
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
;	http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

(letrec
    ((input-object (_make_wmr_reducer_input_object_))
     (help
      (lambda ()
        (if (input-object 'has-next) 
            (let* ((key (input-object 'get-key))
                   (val-stream (_make_wmr_stream_ key input-object)))
              (reducer key val-stream)
              ; Drop any remaining elements in the stream
              (stream-for-each (lambda (x) #t) val-stream)
              (help))
            ;; assert -- no more input
            #f))))
  (help))
