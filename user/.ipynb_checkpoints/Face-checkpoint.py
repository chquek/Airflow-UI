import pprint
import logging

import cv2
import os

os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
import mediapipe as mp

logger = logging.getLogger("kinisi")

def info ( *args , **context ) :
    task = context['dtp']
    print ("Form = " , task.form)
    print ("Task Instance" , context['ti'])
    print ("DAG params" , context['params'] )
    print ("TASK params" , task.form.task_params )
    for i in range(20) :
        try :
            print (f"DATA-{i} - {task.stream[i]}" )
        except :
            pass
    return None

def mesh( img ) :

    mpDraw = mp.solutions.drawing_utils
    mpFaceMesh = mp.solutions.face_mesh
    faceMesh = mpFaceMesh.FaceMesh( max_num_faces=2 )
    drawspec = mpDraw.DrawingSpec ( thickness=1 , circle_radius=1 )
    
    results = faceMesh.process(img)
    if results.multi_face_landmarks:
        for faceLms in results.multi_face_landmarks:
            mpDraw.draw_landmarks(img, faceLms, mpFaceMesh.FACEMESH_CONTOURS, drawspec, drawspec)
            for id,lm in enumerate(faceLms.landmark) :
                # print (lm)
                h , w , c = img.shape        # width , height , channel
                x , y = int(lm.x*w) , int ( lm.y*h )        # convert landmark to pixel
                print ( id , x , y  )

def modifyimage ( *args , **context ) :
    task = context['dtp']
    print ("Form = " , task.form.task_params.filename)
    img = cv2.imread( task.form.task_params.filename ) 
    mesh(img)
    cv2.imwrite ( "/workarea/face_modified.jpeg" , img ) 