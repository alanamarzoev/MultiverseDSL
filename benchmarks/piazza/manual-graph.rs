
fn manual_policy_graph() {
    // Graph for the policy "Reviewer R can see reviews for paper P only if R has submitted
    // a review for P."
    // set up graph
    let mut b = Builder::default();
    b.set_sharding(None);
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        Duration::from_millis(1),
        Some(String::from("manual_policy_graph")),
        1,
    ));
    println!("building graph");
    let mut g = b.start_simple().unwrap();
    let (review, review_assgn) = g.migrate(|mig| {
        // paper,reviewer col is a hacky way of doing multi-column joins
        let review_assgn = mig.add_base(
            "review_assgn",
            &["paper", "reviewer", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![2]));
        let review = mig.add_base(
            "review",
            &["paper", "reviewer", "contents", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![3]));

        (review, review_assgn)
    });

    let (r_ra_join, r_rewrite) = g.migrate(move |mig| {
        let r_ra_join = mig.add_ingredient(
            "r_ra_join",
            &["reviewer", "paper", "contents", "paper,reviewer"],
            Join::new(review, review_assgn, JoinType::Inner, vec![L(1), L(0), L(2), B(3, 2)]));
        let r_rewrite = mig.add_ingredient(
            "r_rewrite",
            &["paper", "reviewer", "contents", "paper,reviewer"],
            Rewrite::new(
                review,
                review,
                1 as usize,
                "anonymous".into(),
                0 as usize));
        (r_ra_join, r_rewrite)
    });
    
    let filter = g.migrate(move |mig| {
        let filter = mig.add_ingredient(
            "filter",
            &["reviewer", "paper", "contents", "paper,reviewer"],
            Filter::new(r_ra_join,
                        &[Some(FilterCondition::In(vec!["r2".into()]))]
            ));
        filter
    });

    let _ = g.migrate(move |mig| {
        let bottom_join = mig.add_ingredient(
            "bottom_join",
            &["paper", "reviewer", "contents"],
            Join::new(r_rewrite, filter, JoinType::Inner, vec![B(0, 1), L(1), L(2)]));

        mig.maintain_anonymous(bottom_join, &[0]);
        
        bottom_join
    });
    
    // populate base tables
    println!("getting table views");
    let mut mutreview = g.table("review").unwrap().into_sync();
    let mut mutrevassgn = g.table("review_assgn").unwrap().into_sync();

    println!("populating base tables");
    mutrevassgn.insert(vec!["2".into(), "r1".into(), "2,r1".into()]).unwrap();
    mutrevassgn.insert(vec!["2".into(), "r2".into(), "2,r2".into()]).unwrap();
    mutrevassgn.insert(vec!["3".into(), "r2".into(), "3,r2".into()]).unwrap();

    mutreview.insert(vec!["2".into(), "r1".into(), "great paper".into(), "2,r1".into()]).unwrap();
    mutreview.insert(vec!["2".into(), "r2".into(), "interesting".into(), "2,r2".into()]).unwrap();
    
    // allow time to propagate
    sleep();
    
    // print graphviz graph representation
    println!("{}", g.graphviz().unwrap());
    
    // send query + verify result
    let mut qview = g.view("bottom_join").unwrap().into_sync();
    let result = qview.lookup(&["2".into()], true).unwrap();
    assert_eq!(result, vec![
        vec!["2".into(), "anonymous".into(), "great paper".into()],
        vec!["2".into(), "anonymous".into(), "interesting".into()]]);
    let result = qview.lookup(&["3".into()], true).unwrap();
    assert_eq!(result.len(), 0);
}


#[test]
fn manual_policy_graph_complete() {
    // Graph for complete set of JConf policies + PC member no-conflict
    // policy on reviews.
    // set up graph
    let mut b = Builder::default();
    b.set_sharding(None);
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        Duration::from_millis(1),
        Some(String::from("manual_policy_graph")),
        1,
    ));
    println!("building graph");
    let mut g = b.start_simple().unwrap();
    // BASE TABLES
    let (review, review_assgn, user_profile, paper_pc_conflict, paper, coauthor) = g.migrate(|mig| {
        // paper,reviewer col is a hacky way of doing multi-column joins
        let review_assgn = mig.add_base(
            "review_assgn",
            &["paper", "reviewer", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![2]));
        let review = mig.add_base(
            "review",
            &["paper", "reviewer", "contents", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![3]));
        let user_profile = mig.add_base(
            "user_profile",
            &["level", "username"],
            Base::new(vec![]).with_key(vec![1]));
        let paper_pc_conflict = mig.add_base(
            "paper_pc_conflict",
            &["username", "paper", "username,paper"],
            Base::new(vec![]).with_key(vec![2])); // needs to be over multiple keys?
        let paper = mig.add_base(
            "paper",
            &["paper","author","accepted"],
            Base::new(vec![]).with_key(vec![0]));
        let coauthor = mig.add_base(
            "coauthor",
            &["paper","author", "paper,author"],
            Base::new(vec![]).with_key(vec![2])); // needs to be over multiple keys?
        (review, review_assgn, user_profile, paper_pc_conflict, paper, coauthor)
    });

    // BASE TABLE DIRECT DERIVATIVES
    let (papers_for_authors, submitted_reviews, pc_members) = g.migrate(move |mig| {
        let paper_rewrite = mig.add_ingredient(
            "paper_rewrite",
            &["paper", "author", "accepted"],
            Rewrite::new(
                paper,
                paper,
                1 as usize,
                "anonymous".into(),
                0 as usize));
        mig.maintain_anonymous(paper_rewrite, &[0]);

        let papers_for_authors = mig.add_ingredient(
            "papers_for_authors",
            &["author", "paper", "accepted"],
            Join::new(paper, coauthor, JoinType::Inner, vec![R(1), B(0, 0), L(2)]));
        
        let submitted_reviews = mig.add_ingredient(
            "submitted_reviews",
            &["reviewer", "paper", "contents", "paper,reviewer"],
            Join::new(review, review_assgn, JoinType::Inner, vec![L(1), L(0), L(2), B(3, 2)]));

        // FOR DEBUGGING
        mig.maintain_anonymous(submitted_reviews, &[0]);
        
        let pc_members = mig.add_ingredient(
            "pc_members",
            &["level", "username"], // another way to specify column other than reversing order?
            Filter::new(user_profile,
                        &[Some(FilterCondition::Comparison(Operator::Equal, Value::Constant("pc".into())))]));

        (papers_for_authors, submitted_reviews, pc_members)
    });

    // NEXT LAYER
    let (reviews_by_r1, review_rewrite, conflicts) = g.migrate(move |mig| {
        let papers_a1 = mig.add_ingredient(
            "papers_a1",
            &["author", "paper", "accepted"], // another way to specify col? 
            Filter::new(papers_for_authors,
                        &[Some(FilterCondition::Comparison(Operator::Equal, Value::Constant("a1".into())))]));
        mig.maintain_anonymous(papers_a1, &[0]);

        let reviews_by_r1 = mig.add_ingredient(
            "reviews_by_r1",
            &["reviewer", "paper", "contents"], // another way to specify col?
            Filter::new(submitted_reviews,
                        &[Some(FilterCondition::Comparison(Operator::Equal, Value::Constant("r1".into())))]));

        // FOR DEBUGGING
        mig.maintain_anonymous(reviews_by_r1, &[0]);

        // Note: anonymization doesn't happen if signal column comes from submitted_reviews
        // instead of directly from review table.
        let review_rewrite = mig.add_ingredient(
            "review_rewrite",
            &["reviewer", "paper", "contents"],
            Rewrite::new(
                submitted_reviews,
                review,
                0 as usize,
                "anonymous".into(),
                1 as usize));

        // FOR DEBUGGING
        mig.maintain_anonymous(review_rewrite, &[1]);
        
        let conflicts = mig.add_ingredient(
            "conflicts",
            &["username", "paper"],
            Join::new(pc_members, paper_pc_conflict, JoinType::Inner, vec![B(1, 0), R(1)]));

        // FOR DEBUGGING
        mig.maintain_anonymous(conflicts, &[1]);
        (reviews_by_r1, review_rewrite, conflicts)
    });

    // NEXT LAYER
    let (revrs_and_conflicts, conflicted_revrs) = g.migrate(move |mig| {
        let reviews_r1 = mig.add_ingredient(
            "reviews_r1",
            &["reviewer", "paper", "contents"],
            Join::new(review_rewrite, reviews_by_r1, JoinType::Inner, vec![L(0), B(1, 1), L(2)]));
        mig.maintain_anonymous(reviews_r1, &[1]);

        /*
        // This is the correct node expression if there is a downstream filter on pc_member
        let revrs_and_conflicts = mig.add_ingredient(
            "revrs_and_conflicts",
            &["pc_member", "reviewer", "paper"],
            Join::new(review_assgn, conflicts, JoinType::Left, vec![R(0), L(1), B(0, 1)]));
         */
        let revrs_and_conflicts = mig.add_ingredient(
            "revrs_and_conflicts",
            &["reviewer", "paper", "pc_member"],
            Join::new(review_assgn, conflicts, JoinType::Left, vec![L(1), B(0, 1), R(0)]));

        let conflicted_revrs = mig.add_ingredient(
            "conflicted_revrs",
            &["pc_member", "reviewer", "paper"],
            Join::new(review_assgn, conflicts, JoinType::Inner, vec![R(0), L(1), B(0, 1)]));

        // FOR DEBUGGING
        mig.maintain_anonymous(conflicted_revrs, &[1]);
        mig.maintain_anonymous(revrs_and_conflicts, &[0]);
        
        (revrs_and_conflicts, conflicted_revrs)
    });

    // FILTER-BY-PC + REMOVE NOT NULL LAYER
    let (non_conflicted_revrs, conflicted_revrs_pc1) = g.migrate(move |mig| {
        // Todo: how to do the "==None" comparison? or at the least, get this !=pc1 comparison working?
        let non_conflicted_revrs = 5;
//        let non_conflicted_revrs = mig.add_ingredient(
//            "non_conflicted_revrs",
//            &["pc_member", "reviewer", "paper"],
//            Filter::new(revrs_and_conflicts,
//                        &[Some(FilterCondition::In(vec!["pc1".into()]))]));
//                        &[Some(FilterCondition::Comparison(Operator::NotEqual,
//                        Value::Constant("pc1".into())))]));
        // fOR DEBUGGING
//        mig.maintain_anonymous(non_conflicted_revrs, &[0]);
        let conflicted_revrs_pc1 = mig.add_ingredient(
            "conflicted_revrs_pc1",
            &["pc_member", "reviewer", "paper"],
            Filter::new(conflicted_revrs,
                        &[Some(FilterCondition::Comparison(Operator::NotEqual,
                                                           Value::Constant("pc1".into())))]));
        // FOR DEBUGGING
        mig.maintain_anonymous(conflicted_revrs_pc1, &[0]);
        (non_conflicted_revrs, conflicted_revrs_pc1)
    });

    // PC REVIEWER VIEW LAYER
    println!("making union node");
    let _ = g.migrate(move |mig| {
        let mut emit = HashMap::new();
        //        emit.insert(non_conflicted_revrs, vec![1 as usize, 2 as usize]);
        emit.insert(revrs_and_conflicts, vec![0 as usize, 1 as usize]);
        emit.insert(conflicted_revrs_pc1, vec![1 as usize, 2 as usize]);
        let reviewers_pc1 = mig.add_ingredient(
            "reviewers_pc1",
            &["reviewer", "paper"],
            Union::new(emit));
        mig.maintain_anonymous(reviewers_pc1, &[0]);
    });

    // Populate Base Tables
    println!("populating base tables");
    let mut mutreview = g.table("review").unwrap().into_sync();
    let mut mutrevassgn = g.table("review_assgn").unwrap().into_sync();
    let mut mutpaper = g.table("paper").unwrap().into_sync();
    let mut mutcoauthor = g.table("coauthor").unwrap().into_sync();
    let mut mutuser_profile = g.table("user_profile").unwrap().into_sync();
    let mut mutpaper_pc_conflict = g.table("paper_pc_conflict").unwrap().into_sync();

    // Schemas:
    // review_assgn: "paper", "reviewer", "paper,reviewer"
    // review: "paper", "reviewer", "contents", "paper,reviewer"
    // user_profile: "username", "level"
    // paper_pc_conflict: "username", "paper", "username,paper"
    // paper: "paper","author","accepted"
    // coauthor: "paper","author", "paper,author"
    
    mutuser_profile.insert(vec!["none".into(), "a1".into()]).unwrap();
    mutuser_profile.insert(vec!["none".into(), "a2".into()]).unwrap();
    mutuser_profile.insert(vec!["none".into(), "a3".into()]).unwrap();
    mutuser_profile.insert(vec!["none".into(), "r1".into()]).unwrap();
    mutuser_profile.insert(vec!["none".into(), "r2".into()]).unwrap();
    mutuser_profile.insert(vec!["none".into(), "r3".into()]).unwrap();
    mutuser_profile.insert(vec!["pc".into(), "pc1".into()]).unwrap();

    mutpaper.insert(vec!["1".into(), "a1".into(), "0".into()]).unwrap();
    mutpaper.insert(vec!["2".into(), "a2".into(), "0".into()]).unwrap();
    mutpaper.insert(vec!["3".into(), "a3".into(), "0".into()]).unwrap();

    mutcoauthor.insert(vec!["1".into(), "a1".into(), "1,a1".into()]).unwrap();
    mutcoauthor.insert(vec!["2".into(), "a1".into(), "2,a1".into()]).unwrap();
    mutcoauthor.insert(vec!["2".into(), "a2".into(), "2,a2".into()]).unwrap();
    mutcoauthor.insert(vec!["3".into(), "a3".into(), "3,a3".into()]).unwrap();
    
    mutrevassgn.insert(vec!["1".into(), "r1".into(), "1,r1".into()]).unwrap();
    mutrevassgn.insert(vec!["3".into(), "r1".into(), "3,r1".into()]).unwrap();
    mutrevassgn.insert(vec!["1".into(), "r2".into(), "1,r2".into()]).unwrap();
    mutrevassgn.insert(vec!["2".into(), "r3".into(), "2,r3".into()]).unwrap();

    mutreview.insert(vec!["1".into(), "r1".into(), "Great paper".into(), "1,r1".into()]).unwrap();
    mutreview.insert(vec!["1".into(), "r2".into(), "Hard to understand".into(), "1,r2".into()]).unwrap();

    mutpaper_pc_conflict.insert(vec!["pc1".into(), "2".into(), "pc1,2".into()]).unwrap();
    // TODO: make a pc2 with a conflict, check it doesn't affect pc1's results
    
    // Allow time to propagate
    sleep();
    
    // Print graphviz graph representation
    println!("{}", g.graphviz().unwrap());
    
    // Query papers for a1, r1, pc1; check results
    let mut t1 = g.view("conflicted_revrs").unwrap().into_sync();
    println!("conflicted_revrs: {:#?}, {:#?}, {:#?}",
             t1.lookup(&["r1".into()], true).unwrap(),
             t1.lookup(&["r2".into()], true).unwrap(),
             t1.lookup(&["r3".into()], true).unwrap());
    let mut t2 = g.view("revrs_and_conflicts").unwrap().into_sync();
    println!("revrs_and_conflicts: {:#?}, {:#?}, {:#?}",
             t2.lookup(&["r1".into()], true).unwrap(),
             t2.lookup(&["r2".into()], true).unwrap(),
             t2.lookup(&["r3".into()], true).unwrap());
    let mut t3 = g.view("conflicts").unwrap().into_sync();
    println!("conflicts: {:#?}, {:#?}, {:#?}",
             t3.lookup(&["1".into()], true).unwrap(),
             t3.lookup(&["2".into()], true).unwrap(),
             t3.lookup(&["3".into()], true).unwrap());

    let mut papers_a1_view = g.view("papers_a1").unwrap().into_sync();
    assert_eq!(papers_a1_view.lookup(&["a1".into()], true).unwrap(),
               vec![vec!["a1".into(), "1".into(), "0".into()],
                    vec!["a1".into(), "2".into(), "0".into()]]);

    let mut paper_rw_view = g.view("paper_rewrite").unwrap().into_sync();
    let expected = vec![vec![vec!["1".into(), "anonymous".into(), "0".into()]],
                        vec![vec!["2".into(), "anonymous".into(), "0".into()]],
                        vec![vec!["3".into(), "anonymous".into(), "0".into()]]];
    for i in 1..4 {
        assert_eq!(paper_rw_view.lookup(&[i.to_string().into()], true).unwrap(), expected[i-1]);
    }
    
    // Query reviews for a1, r1, pc1; check results
    let mut reviews_r1_view = g.view("reviews_r1").unwrap().into_sync();
    assert_eq!(reviews_r1_view.lookup(&["1".into()], true).unwrap(),
               vec![vec!["anonymous".into(), "1".into(), "Great paper".into()],
                    vec!["anonymous".into(), "1".into(), "Hard to understand".into()]]);
    assert_eq!(reviews_r1_view.lookup(&["3".into()], true).unwrap().len(), 0);

    let mut revrs_pc1_view = g.view("reviewers_pc1").unwrap().into_sync();
    let expected = vec![
        vec![vec!["r1".into(), "1".into()], vec!["r1".into(), "3".into()]],
        vec![vec!["r2".into(), "1".into()]],
        vec![]];
    // TODO: why is lookup empty? revrs_and_conflicts has 4 entries...
    for i in 1..4 { 
        let revr = format!("{}{}", "r", i.to_string());
        assert_eq!(revrs_pc1_view.lookup(&[revr.into()], true).unwrap(), expected[i-1]);
    }
}


#[test]
fn manual_policy_graph_jconf() {
    // Graph for complete set of JConf policies.
    // set up graph
    let mut b = Builder::default();
    b.set_sharding(None);
    b.set_persistence(PersistenceParameters::new(
        DurabilityMode::DeleteOnExit,
        Duration::from_millis(1),
        Some(String::from("manual_policy_graph")),
        1,
    ));
    println!("building graph");
    let mut g = b.start_simple().unwrap();
    // BASE TABLES
    let (review, review_assgn, paper, coauthor) = g.migrate(|mig| {
        // paper,reviewer col is a hacky way of doing multi-column joins
        let review_assgn = mig.add_base(
            "review_assgn",
            &["paper", "reviewer", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![2]));
        mig.maintain_anonymous(review_assgn, &[1]); // for PC members to view
        let review = mig.add_base(
            "review",
            &["paper", "reviewer", "contents", "paper,reviewer"],
            Base::new(vec![]).with_key(vec![3]));
        let paper = mig.add_base(
            "paper",
            &["paper","author","accepted"],
            Base::new(vec![]).with_key(vec![0]));
        let coauthor = mig.add_base(
            "coauthor",
            &["paper","author", "paper,author"],
            Base::new(vec![]).with_key(vec![2])); // needs to be over multiple keys?
        (review, review_assgn, paper, coauthor)
    });

    // BASE TABLE DIRECT DERIVATIVES
    let (paper_rewrite, papers_for_authors, submitted_reviews) = g.migrate(move |mig| {
        let paper_rewrite = mig.add_ingredient(
            "paper_rewrite",
            &["paper", "author", "accepted"],
            Rewrite::new(
                paper,
                paper,
                1 as usize,
                "anonymous".into(),
                0 as usize));
        mig.maintain_anonymous(paper_rewrite, &[0]);

        let papers_for_authors = mig.add_ingredient(
            "papers_for_authors",
            &["author", "paper", "accepted"],
            Join::new(paper, coauthor, JoinType::Inner, vec![R(1), B(0, 0), L(2)]));
        
        let submitted_reviews = mig.add_ingredient(
            "submitted_reviews",
            &["reviewer", "paper", "contents", "paper,reviewer"],
            Join::new(review, review_assgn, JoinType::Inner, vec![L(1), L(0), L(2), B(3, 2)]));

        (paper_rewrite, papers_for_authors, submitted_reviews)
    });

    // NEXT LAYER
    let (reviews_by_r1, review_rewrite) = g.migrate(move |mig| {
        let papers_a1 = mig.add_ingredient(
            "papers_a1",
            &["author", "paper", "accepted"], // another way to specify col? 
            Filter::new(papers_for_authors,
                        &[Some(FilterCondition::Comparison(Operator::Equal, Value::Constant("a1".into())))]));
        mig.maintain_anonymous(papers_a1, &[0]);

        let reviews_by_r1 = mig.add_ingredient(
            "reviews_by_r1",
            &["reviewer", "paper", "contents"], // another way to specify col?
            Filter::new(submitted_reviews,
                        &[Some(FilterCondition::Comparison(Operator::Equal, Value::Constant("r1".into())))]));

        // Note: anonymization doesn't happen if signal column comes from submitted_reviews
        // instead of directly from review table.
        let review_rewrite = mig.add_ingredient(
            "review_rewrite",
            &["reviewer", "paper", "contents"],
            Rewrite::new(
                submitted_reviews,
                review,
                0 as usize,
                "anonymous".into(),
                1 as usize));
        
        (reviews_by_r1, review_rewrite)
    });

    // REVIEWS FOR R1
    let reviews_r1 = g.migrate(move |mig| {
        let reviews_r1 = mig.add_ingredient(
            "reviews_r1",
            &["reviewer", "paper", "contents"],
            Join::new(review_rewrite, reviews_by_r1, JoinType::Inner, vec![L(0), B(1, 1), L(2)]));
        mig.maintain_anonymous(reviews_r1, &[1]);

        reviews_r1
    });

    // REVIEWLIST QUERY
    let _ = g.migrate(move |mig| {
        let revlist_r1 = mig.add_ingredient(
            "revlist_r1",
            &["paper", "reviewer", "contents", "author", "accepted"],
            Join::new(paper_rewrite, reviews_r1,
                      JoinType::Inner, vec![B(0, 1), R(0), R(2), L(1), L(2)]));
        mig.maintain_anonymous(revlist_r1, &[0]);
    });

    // Populate Base Tables
    println!("populating base tables");
    let mut mutreview = g.table("review").unwrap().into_sync();
    let mut mutrevassgn = g.table("review_assgn").unwrap().into_sync();
    let mut mutpaper = g.table("paper").unwrap().into_sync();
    let mut mutcoauthor = g.table("coauthor").unwrap().into_sync();

    // Schemas:
    // review_assgn: "paper", "reviewer", "paper,reviewer"
    // review: "paper", "reviewer", "contents", "paper,reviewer"
    // user_profile: "username", "level"
    // paper_pc_conflict: "username", "paper", "username,paper"

    mutpaper.insert(vec!["1".into(), "a1".into(), "0".into()]).unwrap();
    mutpaper.insert(vec!["2".into(), "a2".into(), "0".into()]).unwrap();
    mutpaper.insert(vec!["3".into(), "a3".into(), "0".into()]).unwrap();

    mutcoauthor.insert(vec!["1".into(), "a1".into(), "1,a1".into()]).unwrap();
    mutcoauthor.insert(vec!["2".into(), "a1".into(), "2,a1".into()]).unwrap();
    mutcoauthor.insert(vec!["2".into(), "a2".into(), "2,a2".into()]).unwrap();
    mutcoauthor.insert(vec!["3".into(), "a3".into(), "3,a3".into()]).unwrap();
    
    mutrevassgn.insert(vec!["1".into(), "r1".into(), "1,r1".into()]).unwrap();
    mutrevassgn.insert(vec!["3".into(), "r1".into(), "3,r1".into()]).unwrap();
    mutrevassgn.insert(vec!["1".into(), "r2".into(), "1,r2".into()]).unwrap();
    mutrevassgn.insert(vec!["2".into(), "r3".into(), "2,r3".into()]).unwrap();

    mutreview.insert(vec!["1".into(), "r1".into(), "Great paper".into(), "1,r1".into()]).unwrap();
    mutreview.insert(vec!["1".into(), "r2".into(), "Hard to understand".into(), "1,r2".into()]).unwrap();
    
    // Allow time to propagate
    sleep();
    
    // Print graphviz graph representation
    println!("{}", g.graphviz().unwrap());
    
    // Query papers for a1, r1, pc1; check results
    let mut papers_a1_view = g.view("papers_a1").unwrap().into_sync();
    assert_eq!(papers_a1_view.lookup(&["a1".into()], true).unwrap(),
               vec![vec!["a1".into(), "1".into(), "0".into()],
                    vec!["a1".into(), "2".into(), "0".into()]]);

    let mut paper_rw_view = g.view("paper_rewrite").unwrap().into_sync();
    let expected = vec![vec![vec!["1".into(), "anonymous".into(), "0".into()]],
                        vec![vec!["2".into(), "anonymous".into(), "0".into()]],
                        vec![vec!["3".into(), "anonymous".into(), "0".into()]]];
    for i in 1..4 {
        assert_eq!(paper_rw_view.lookup(&[i.to_string().into()], true).unwrap(), expected[i-1]);
    }
    
    // Query reviews for a1, r1, pc1; check results
    let mut reviews_r1_view = g.view("reviews_r1").unwrap().into_sync();
    assert_eq!(reviews_r1_view.lookup(&["1".into()], true).unwrap(),
               vec![vec!["anonymous".into(), "1".into(), "Great paper".into()],
                    vec!["anonymous".into(), "1".into(), "Hard to understand".into()]]);
    assert_eq!(reviews_r1_view.lookup(&["3".into()], true).unwrap().len(), 0);

    let mut revassgn_view = g.view("review_assgn").unwrap().into_sync();
    assert_eq!(revassgn_view.lookup(&["r1".into()], true).unwrap(),
               vec![vec!["1".into(), "r1".into(), "1,r1".into()],
                    vec!["3".into(), "r1".into(), "3,r1".into()]]);
    assert_eq!(revassgn_view.lookup(&["r2".into()], true).unwrap(),
               vec![vec!["1".into(), "r2".into(), "1,r2".into()]]);
    assert_eq!(revassgn_view.lookup(&["r3".into()], true).unwrap(),
               vec![vec!["2".into(), "r3".into(), "2,r3".into()]]);
    
    // Check ReviewList for r1 (authors and pc members in this phase can't see any reviews)
    let mut revlist_r1_view = g.view("revlist_r1").unwrap().into_sync();
    assert_eq!(revlist_r1_view.lookup(&["1".into()], true).unwrap(),
               vec![vec!["1".into(), "anonymous".into(), "Great paper".into(),
                         "anonymous".into(), "0".into()],
                    vec!["1".into(), "anonymous".into(), "Hard to understand".into(),
                         "anonymous".into(), "0".into()]]);
    assert_eq!(revlist_r1_view.lookup(&["3".into()], true).unwrap().len(), 0);
}